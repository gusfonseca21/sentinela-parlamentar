import asyncio
import hashlib
import json
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from prefect import get_run_logger
from prefect.exceptions import MissingContextError

from config.loader import load_config

APP_SETTINGS = load_config()


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
    progress_artifact_id: UUID | None = None,
) -> str:
    """
    Faça o download de um arquivo em stream e opcionalmente extrai os arquivos, caso seja um ZIP.
    Retorna o caminho do arquivo.
    """
    dest_path = Path(dest_path)
    ensure_dir(dest_path.parent)
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
    out_dir: str | Path | None = None,
    concurrency: int = 10,
    timeout: int = 1800,
    logger: Any | None = None,
) -> list[str | None] | str:
    """
    Faz o download de páginas HTML
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

    processed_urls = set()  # Evita processar a mesma URL duas vezes

    downloaded_urls = 0

    async def one(u: str, client: httpx.AsyncClient):
        nonlocal downloaded_urls

        if u in processed_urls:
            return None
        processed_urls.add(u)

        async with sem:
            log(f"Fazendo download da URL: {u}")
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

    async with httpx.AsyncClient(limits=limits, timeout=timeout_cfg) as client:
        tasks = [one(u, client) for u in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Elimina os resultados inválidos (erro)
        valid_results = [
            result for result in results if not isinstance(result, BaseException)
        ]

    return valid_results
