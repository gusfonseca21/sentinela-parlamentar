import asyncio
from pathlib import Path
from typing import Any

import httpx

from .io import ensure_dir
from .log import get_prefect_logger_or_none
from .url_utils import alter_query_param_value, get_query_param_value, is_first_page


# Armazena em memória ou grava em disco uma lista de JSONs
async def fetch_many_camara(
    urls: list[str],
    out_dir: str | Path | None = None,
    limit: int = 10,
    timeout: float = 30.0,
    max_retries: int = 10,
    follow_pagination: bool = False,
    logger: Any | None = None,
    validate_results: bool = False,
) -> list[str] | list[dict]:
    """
    - Se out_dir for fornecido, salva cada JSON em um arquivo e retorna a lista de caminhos
    - Caso contrário, retorna a lista de dicionários em memória
    """
    logger = logger or get_prefect_logger_or_none()

    def log(msg: str):
        if logger:
            logger.warning(msg)
        else:
            print(msg)

    out_dir = ensure_dir(out_dir) if out_dir else None

    async def worker(
        queue: asyncio.Queue,
        results: list[dict],
        processed_urls: set,
        semaphore: asyncio.Semaphore,
        client: httpx.AsyncClient,
        stats: dict,
    ):
        while True:  # Mantém o consumidor da fila vivo para processar outras urls
            url = await queue.get()

            if url in processed_urls:
                queue.task_done()
                continue

            # Adiciona logo em processed_urls para evitar que o queue pegue essa url
            processed_urls.add(url)

            async with semaphore:
                print(f"Baixando URL: {url=}")
                for attempt in range(max_retries):
                    try:
                        response = await client.get(url, timeout=timeout)

                        response.raise_for_status()
                        data = response.json()

                        if is_first_page(url):
                            total_items = response.headers.get("x-total-count", None)

                            if total_items:
                                stats["total_items"] += int(total_items)

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

                        queue.task_done()
                        break
                    except Exception as e:
                        if attempt < max_retries - 1:
                            log(
                                f"Um erro ocorreu no fetch de dados: {e}. TENTANDO NOVAMENTE. Tentativa: {attempt}"
                            )
                            await asyncio.sleep(2**attempt)
                        else:
                            queue.task_done()
                            message = f"Falha permanente ao baixar {url} após {max_retries} tentativas: {e}"
                            log(message)
                            raise Exception(message)

    queue = asyncio.Queue()

    for u in urls:
        await queue.put(u)

    processed_urls = set()
    results = []
    stats = {"total_items": 0}

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
                    stats,
                )
            )
            for _ in range(
                int(limit)
            )  # Cria um pouco mais de workers do que conexões abertas simultâneas
        ]

        await queue.join()

    for w in workers:
        w.cancel()

    if validate_results:
        validate(
            results=results,
            urls=urls,
            stats=stats,
            log=log,
            paginated=follow_pagination,
        )

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


def validate(
    results: list[dict],
    urls: list[str],
    stats: dict[str, int],
    log: Any,
    paginated: bool,
):
    downloaded_items = 0

    if paginated:
        for page in results:
            page_items = len(page.get("dados", []))
            downloaded_items += page_items
    else:
        downloaded_items = len(results)
        stats["total_items"] = len(urls)

    if stats["total_items"]:
        log(
            f"Total de ítens baixados / headers/lenlista: {downloaded_items}/{stats['total_items']}"
        )
    else:
        log(
            f"O header do total de items para serem baixados não foi encontrado. Total de downloads: {downloaded_items}"
        )

    if (
        stats["total_items"]  # Se não possuir header, não valida
        and stats["total_items"] != downloaded_items
    ):
        raise Exception(
            f"ERRO: O NÚMERO DE ITENS BAIXADOS É DIFERENTE DO NÚMERO TOTAL:\n Baixados: {downloaded_items}/{stats['total_items']}"
        )
