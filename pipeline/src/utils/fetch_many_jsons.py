import asyncio
from pathlib import Path

import httpx
from prefect.logging import get_logger

from config.request_headers import headers
from database.models.base import ErrorExtract
from database.repository.erros_extract import (
    insert_extract_error_db,
    update_not_downloaded_urls_db,
)

from .io import ensure_dir
from .url_utils import alter_query_param_value, get_query_param_value, is_first_page

logger = get_logger()


# Armazena em memória ou grava em disco uma lista de JSONs
async def fetch_many_jsons(
    urls: list[str],
    not_downloaded_urls: list[ErrorExtract],
    task: str,
    lote_id: int,
    out_dir: str | Path | None = None,
    limit: int = 10,
    timeout: float = 30.0,
    max_retries: int = 10,
    follow_pagination: bool = False,
    validate_results: bool = False,
) -> list[str] | list[dict]:
    """
    - Se out_dir for fornecido, salva cada JSON em um arquivo e retorna a lista de caminhos
    - Caso contrário, retorna a lista de dicionários em memória
    """

    db_errors = []
    out_dir = ensure_dir(out_dir) if out_dir else None

    async def worker(
        queue: asyncio.Queue,
        results: list[dict],
        processed_urls: set,
        semaphore: asyncio.Semaphore,
        client: httpx.AsyncClient,
        stats: dict,
        task: str,
        lote_id: int,
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

                # status_code = None
                # request_message = None

                for attempt in range(max_retries):
                    try:
                        response = await client.get(url, timeout=timeout)

                        status_code = response.status_code

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

                            # Verificar e atualizar no banco de dados as urls com falhas
                            failed_urls = {
                                error.url: error for error in not_downloaded_urls
                            }
                            if failed_urls:
                                try:
                                    update_url_not_downloaded(url, failed_urls)
                                except Exception as e:
                                    logger.critical(
                                        f"Não foi possível atualizar o registro de URL baixada no banco de dados: {e}"
                                    )
                                    db_errors.append(url)

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
                            logger.warning(
                                f"Um erro ocorreu no fetch de dados: {e}. TENTANDO NOVAMENTE. Tentativa: {attempt}"
                            )
                            await asyncio.sleep(2**attempt)
                        else:
                            queue.task_done()
                            message = f"Falha permanente ao baixar {url} após {max_retries} tentativas: {e}"
                            logger.error(message)

                            try:
                                status_code = (
                                    e.response.status_code
                                    if isinstance(e, httpx.HTTPStatusError)
                                    else None
                                )

                                insert_extract_error_db(
                                    lote_id=lote_id,
                                    task=task,
                                    status_code=status_code,
                                    message=str(e),
                                    url=url,
                                )
                            except Exception as e:
                                # Não damos raise aqui pois daremos um tratamento próprio para as URLs que não foram baixadas
                                logger.critical(
                                    f"Erro ao tentar inserir o erro da URL {url} no banco de dados: {e}"
                                )
                                db_errors.append(url)

    queue = asyncio.Queue()

    for u in urls:
        await queue.put(u)

    processed_urls = set()
    results = []
    stats = {"total_items": 0}

    semaphore = asyncio.Semaphore(limit)

    async with httpx.AsyncClient(headers=headers) as client:
        workers = [
            asyncio.create_task(
                worker(
                    queue,
                    results,
                    processed_urls,
                    semaphore,
                    client,
                    stats,
                    task,
                    lote_id,
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
            paginated=follow_pagination,
        )

    if db_errors:
        # Se der erro na hora de criar o registro, damos Raise para avisar
        raise Exception(
            f"Houve erro na inserção de {len(db_errors)} erros de URLs no banco de dados"
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
        logger.info(
            f"Total de ítens baixados / headers/lenlista: {downloaded_items}/{stats['total_items']}"
        )
    else:
        logger.info(
            f"O header do total de items para serem baixados não foi encontrado ou é 0. Total de downloads: {downloaded_items}"
        )

    if (
        stats["total_items"]  # Se não possuir header, não valida
        and stats["total_items"] != downloaded_items
    ):
        raise Exception(
            f"ERRO: O NÚMERO DE ITENS BAIXADOS É DIFERENTE DO NÚMERO TOTAL:\n Baixados: {downloaded_items}/{stats['total_items']}"
        )


def update_url_not_downloaded(url: str, failed_urls: dict[str, ErrorExtract]):
    """
    Atualiza no banco de dados o registro da URL que não havia sido baixada
    """

    if url in failed_urls:
        error = failed_urls[url]
        update_not_downloaded_urls_db(error.id)
