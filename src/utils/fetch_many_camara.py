import asyncio
from pathlib import Path
from typing import Any

import httpx

from config.loader import load_config

from .io import _get_prefect_logger_or_none, ensure_dir
from .url_utils import alter_query_param_value, get_query_param_value

APP_SETTINGS = load_config()


# Armazena em memória ou grava em disco uma lista de JSONs
async def fetch_many_camara(
    urls: list[str],
    out_dir: str | Path | None = None,
    limit: int = 10,
    timeout: float = 30.0,
    follow_pagination: bool = True,
    logger: Any | None = None,
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

    out_dir = ensure_dir(out_dir) if out_dir else None

    async def worker(
        queue: asyncio.Queue,
        results: list[dict],
        processed_urls: set,
        semaphore: asyncio.Semaphore,
        client: httpx.AsyncClient,
        total_items_from_headers: int,
    ):
        while True:  # Mantém o consumidor da fila vivo para processar outras urls
            url = await queue.get()

            if url in processed_urls:
                queue.task_done()
                continue

            async with semaphore:
                for attempt in range(APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES):
                    try:
                        response = await client.get(url, timeout=timeout)

                        response.raise_for_status()
                        data = response.json()

                        # Pega o número de items presentes no json
                        if get_query_param_value(url, "pagina", 0) == 0:
                            # Só é necessário pegar na primeira página
                            total_items = response.headers.get("x-total-count", None)
                            if total_items:
                                total_items_from_headers += int(total_items)

                        if out_dir:
                            raise Exception("O BLOCO out_dir ESTÁ COMENTADO")
                            # name = hashlib.sha1(url.encode()).hexdigest() + ".json"
                            # path = Path(out_dir) / name

                            # # to_thread é usado para evitar que a escrita no disco congele o processo na rede
                            # await asyncio.to_thread(save_json, path, data)
                            # results.append(str(path))
                        else:
                            results.append(data)

                        # Se tiver paginação, adiciona novas URLs à fila
                        if follow_pagination and "links" in data:
                            links = {
                                link["rel"]: link["href"] for link in data["links"]
                            }
                            if "self" in links and "last" in links:
                                for new_url in generate_pages_urls(
                                    links["self"], links["last"]
                                ):
                                    if new_url not in processed_urls:
                                        await queue.put(new_url)

                        processed_urls.add(url)
                        queue.task_done()
                    except Exception as e:
                        if attempt < APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES - 1:
                            log(
                                f"Um erro ocorreu no fetch de dados: {e}. TENTANDO NOVAMENTE. Tentativa: {attempt}"
                            )
                            delay = 2**attempt
                            await asyncio.sleep(delay)
                        else:
                            queue.task_done()
                            raise

    queue = asyncio.Queue()

    for u in urls:
        await queue.put(u)

    processed_urls = set()
    results = []
    total_items_from_headers = 0

    semaphore = asyncio.Semaphore(limit)

    async with httpx.AsyncClient() as client:
        workers = [
            asyncio.create_task(
                worker(
                    queue,
                    results,
                    processed_urls,
                    semaphore,
                    client,
                    total_items_from_headers,
                )
            )
            for _ in range(
                int(limit * 1.5)
            )  # Cria um pouco mais de workers do que conexões abertas simultâneas
        ]

        await queue.join()

    for w in workers:
        w.cancel()

    return results


def generate_pages_urls(url_self: str, url_last: str):
    """
    Caso a url baixada tenha mais páginas, retorna uma lista com as páginas adicionais a serem baixadas
    """
    # Pega o número da primeira página
    self_page = int(get_query_param_value(url_self, "pagina", "1"))

    # Se não for a primeira página, retorna pois todas as URLs já foram geradas
    if self_page > 1:
        return []

    # Pega o número da última página
    last_page = int(
        get_query_param_value(url=url_last, param_name="pagina", default_value="1")
    )

    # Gera as urls das páginas seguintes
    urls = []
    for page in range(2, (last_page + 1)):
        new_url = alter_query_param_value(
            base_url=url_self, param_name="pagina", new_value=page
        )
        urls.append(new_url)

    return urls
