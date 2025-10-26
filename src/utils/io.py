from pathlib import Path
import httpx
import json
import asyncio
import hashlib
from datetime import datetime
from typing import Any
import zipfile
from prefect import get_run_logger
from prefect.exceptions import MissingContextError
from prefect.artifacts import update_progress_artifact, aupdate_progress_artifact

# Garante que o caminho exista
def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def _get_prefect_logger_or_none() -> Any | None:
    try:
        return get_run_logger()
    except MissingContextError:
        return None

# Download de arquivos zip em streaming por conta dos arquivos pesados
def download_stream(
        url: str,
        dest_path: str | Path,
        unzip: bool = False,
        timeout: float = 60.0,
        progress_artifact_id: Any | None = None,
) -> str:
    """
    Faça o download de um arquivo em stream e opcionalmente extrai os arquivos, caso seja um ZIP.
    Retorna o caminho do arquivo.
    """
    dest_path = Path(dest_path)
    ensure_dir(dest_path.parent)
    with httpx.stream("GET", url, timeout=timeout) as r:
        r.raise_for_status()

        total_size = int(r.headers.get("content-length", 0))
        downloaded_size = 0

        with open(dest_path, "wb") as f:
            for chunk in r.iter_bytes():
                f.write(chunk)
                downloaded_size += len(chunk)
                if progress_artifact_id and total_size > 0:
                    update_progress_artifact(artifact_id=progress_artifact_id, progress=(downloaded_size / total_size) * 100)

        if unzip:
            extracted_files = unzip_file(dest_path)
            dest_path.unlink() # Apaga os zips após a extração
            return str(dest_path)
        else:
            return str(dest_path)

def unzip_file(zip_path: str | Path) -> list[str]:
    zip_path = Path(zip_path)
    extract_dir = zip_path.parent

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
        extracted_files = [str(extract_dir / name) for name in zf.namelist()]
    
    return extracted_files


# Busca um json
def fetch_json(url: str, timeout: float = 30.0) -> dict | list:
    """
    Busca um JSON a partir da URL e retorna o objeto em memória
    """
    with httpx.Client(timeout=timeout) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.json()

def save_json(data: Any, dest_path: str | Path, timeout: float = 60.0) -> str:
    """
    Salva um JSON em disco
    """
    dest_path = Path(dest_path)
    ensure_dir(dest_path.parent)
    with open(dest_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    return str(dest_path)

# Salva uma lista de JSONs em um único NDJson
def save_ndjson(records: list[dict], dest_path: str | Path) -> str:
    print(f"Gravando json {datetime.now()}")
    dest_path = Path(dest_path)
    ensure_dir(dest_path.parent)
    with open(dest_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return str(dest_path)

# Armazena em memória ou grava em disco uma lista de JSONs
async def fetch_json_many_async(
        urls: list[str],
        out_dir: str | Path | None = None,
        concurrency: int = 10,
        timeout: float = 30.0,
        follow_pagination: bool = True,
        logger: Any | None = None,
        progress_artifact_id: Any | None = None,
) -> list[str] | list[dict]:
    """
    - Se out_dir for fornecido, salva cada JSON em um arquivo e retorna a lista de caminhos
    - Caso contrário, retorna a lista de dicionários em memória
    """
    logger = logger or _get_prefect_logger_or_none()

    def log(msg: str):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=max(concurrency, 10))
    timeout_cfg = httpx.Timeout(timeout)
    ensure_dir(out_dir) if out_dir else None
    
    processed_urls = set() # Evita processar a mesma URL duas vezes
    results = []

    downloaded_urls = 0
    update_lock = asyncio.Lock() # Evita race conditions ao atualizar o progresso de forma assíncrona

    async def one(u: str):
        nonlocal downloaded_urls

        if u in processed_urls:
            return []
        processed_urls.add(u)

        async with sem:
            async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg) as client:
                log(f"Fazendo download da URL: {u}")
                r = await client.get(u)
                r.raise_for_status()
                data = r.json()

            # Salvar ou retornar o resultado atual
            if out_dir:
                # Nome do arquivo determinado pelo Hash da URL
                name = hashlib.sha1(u.encode()).hexdigest() + ".json"
                path = Path(out_dir) / name
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                current_result = str(path)
            else:
                current_result = data

            if progress_artifact_id and len(urls) > 0:
                async with update_lock:
                    downloaded_urls += 1
                    # Só atualiza a cada 5 urls baixadas, para atualizar o progress bar da UI
                    if downloaded_urls % 5 == 0 or downloaded_urls == len(urls): 
                        await aupdate_progress_artifact(
                            artifact_id=progress_artifact_id,
                            progress=(downloaded_urls / len(urls)) * 100
                        )

        # Verifica se deve serguir a paginação
        additional_results = []
        if follow_pagination and "links" in data:
            links = {link["rel"]: link["href"] for link in data["links"]}

            if "self" in links and "last" in links:
                if links["self"] != links["last"] and "next" in links:
                    next_url = links["next"]
                    additional_results = await one(next_url)

            # Retorna resultado atual + resultados adicionais da paginação
            if isinstance(additional_results, list):
                return [current_result] + additional_results
            else:
                return [current_result]
            
    tasks = [one(u) for u in urls]
    nested_results = await asyncio.gather(*tasks)

    # Achata a lista de resultados
    for item in nested_results:
        if isinstance(item, list):
            results.extend(item)
        else:
            results.append(item)

    return results

