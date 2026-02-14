import asyncio
import hashlib
import json
import os
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any

import httpx
from prefect.logging import get_logger

from config.loader import load_config
from config.request_headers import headers
from database.repository.erros_extract import insert_extract_error_db

APP_SETTINGS = load_config()

logger = get_logger()


# Garante que o caminho exista
def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


# Download de arquivos zip em streaming por conta dos arquivos pesados
def download_stream(
    url: str,
    lote_id: int,
    task: str,
    dest_path: str | Path,
    unzip: bool = False,
    timeout: float = 60.0,
    max_retries: int = APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
) -> str | None:
    """
    Faça o download de um arquivo em stream e opcionalmente extrai os arquivos, caso seja um ZIP.
    Retorna o caminho do arquivo.
    """
    dest_path = Path(dest_path)
    ensure_dir(dest_path.parent)

    for attempt in range(max_retries):
        try:
            with httpx.stream("GET", url, timeout=timeout) as r:
                r.raise_for_status()

                _total_size = int(r.headers.get("content-length", 0))
                downloaded_size = 0

                with open(dest_path, "wb") as f:
                    for chunk in r.iter_bytes():
                        f.write(chunk)
                        downloaded_size += len(chunk)

                if unzip:
                    _extracted_files = unzip_file(dest_path)
                    dest_path.unlink()  # Apaga os zips após a extração
                    return str(dest_path)
                else:
                    return str(dest_path)
        except Exception as e:
            status_code = (
                e.response.status_code if isinstance(e, httpx.HTTPStatusError) else None
            )

            print(
                f"Erro ao tentar baixar arquivos por stream, tentativa {attempt}: {e}"
            )
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
            else:
                # Não damos raise na exceção pois daremos um tratamento próprio para as URLs que não foram baixadas
                message = f"Falha ao baixar o recurso {url} após {max_retries} tentativas: {e}"
                logger.error(message)

                try:
                    insert_extract_error_db(
                        lote_id=lote_id,
                        task=task,
                        status_code=status_code,
                        message=str(e),
                        url=url,
                    )
                except Exception as e:
                    logger.critical(
                        f"Erro ao tentar inserir o erro da URL {url} no banco de dados: {e}"
                    )


def unzip_file(zip_path: str | Path) -> list[str]:
    zip_path = Path(zip_path)
    extract_dir = zip_path.parent

    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
        extracted_files = [str(extract_dir / name) for name in zf.namelist()]

    return extracted_files


# Busca um json
def fetch_json(
    url: str,
    timeout: float = 30.0,
    max_retries: int = 10,
) -> dict | list | None:
    """
    Busca um JSON a partir da URL e retorna o objeto em memória
    """

    logger.info(f"Baixando URL: {url}")

    with httpx.Client(
        timeout=timeout, follow_redirects=True, headers=headers
    ) as client:
        for attempt in range(max_retries):
            try:
                r = client.get(url)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Um erro ocorreu no fetch de dados: {e}. TENTANDO NOVAMENTE. Tentativa: {attempt}"
                    )
                    time.sleep(2**attempt)
                else:
                    message = f"Erro ao baixar recurso da url {url} após {max_retries} tentativas: {e}"
                    logger.error(message)

                    # Aqui jogamos o erro pois normalmente as tasks necessitam dos dados de dados que são baixados de um único JSON.
                    raise Exception(message)


def save_json(data: Any, dest_path: str | Path) -> str:
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
    """
    Salva arquivos no formato NDJson, que agrupa vários JSONS.
    Só grava em disco depois dos dados estiverem consolidados
    """
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")

    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        os.replace(tmp_path, dest_path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
    return str(dest_path)


def merge_ndjson(inputs: list[str | Path], dest: str | Path) -> str:
    """
    Quando temos vários NDJsons da mesma task, fazemos o merge deles em um único arquivo.
    """
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    with open(tmp, "w", encoding="utf-8") as out:
        for p in inputs:
            p = Path(p)
            if not p.exists():
                continue
            with open(p, "r", encoding="utf-8") as f:
                shutil.copyfileobj(f, out)
            os.unlink(p)
    os.replace(tmp, dest)
    return str(dest)


async def fetch_html_many_async(
    urls: list[str],
    lote_id: int,
    task: str,
    out_dir: str | Path | None = None,
    limit: int = 10,
    timeout: int = 1800,
    max_retries: int = 10,
) -> list[str | None]:
    """
    Faz o download de páginas HTML
    """

    sem = asyncio.Semaphore(limit)
    timeout_cfg = httpx.Timeout(timeout)

    ensure_dir(out_dir) if out_dir else None

    processed_urls = set()  # Evita processar a mesma URL duas vezes

    async def fetch(u: str, client: httpx.AsyncClient):
        if u in processed_urls:
            return None
        processed_urls.add(u)

        async with sem:
            for attempt in range(max_retries):
                try:
                    logger.info(f"Fazendo download da URL: {u}")
                    r = await client.get(u)
                    r.raise_for_status()

                    html_content = r.text

                    # Salvar ou retornar o resultado atual
                    if out_dir:
                        # Nome do arquivo determinado pelo Hash da URL
                        name = hashlib.sha1(u.encode()).hexdigest() + ".html"
                        path = Path(out_dir) / name
                        with open(path, "w", encoding="utf-8") as f:
                            f.write(html_content)
                        return str(path)  # Se salvar, retorna o caminho

                    return html_content

                except Exception as e:
                    status_code = (
                        e.response.status_code
                        if isinstance(e, httpx.HTTPStatusError)
                        else None
                    )

                    if attempt < max_retries - 1:
                        logger.warning(
                            f"Um erro ocorreu ao baixar uma página HTML: {e}. TENTANDO NOVAMENTE. Tentativa: {attempt}"
                        )
                        await asyncio.sleep(2**attempt)
                    else:
                        logger.error(
                            f"Falha permanente ao baixar {u} após {max_retries} tentativas: {e}"
                        )

                        try:
                            insert_extract_error_db(
                                lote_id=lote_id,
                                task=task,
                                status_code=status_code,
                                message=str(e),
                                url=u,
                            )
                        except Exception as e:
                            logger.critical(
                                f"Erro ao tentar inserir o erro da URL {u} no banco de dados: {e}"
                            )

    async with httpx.AsyncClient(timeout=timeout_cfg, follow_redirects=True) as client:
        tasks = [fetch(u, client) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Elimina os resultados inválidos (erro)
        valid_results = [
            result for result in results if not isinstance(result, BaseException)
        ]

    return valid_results
