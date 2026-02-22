from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def detalhes_senadores_urls(senadores_ids: list[str]) -> UrlsResult:
    urls = set()

    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_SENADO_DETALHES_SENADORES
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in senadores_ids:
        urls.add(f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/{id}?v=6")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_SENADO_DETALHES_SENADORES,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_detalhes_senadores_senado(
    ids_senadores: list[str],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = detalhes_senadores_urls(ids_senadores)

    logger.info(f"Baixando detalhes de {len(urls)} URLs de Senadores")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.EXTRACT_SENADO_DETALHES_SENADORES,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="detalhes-senadores",
        table=[{"num_senadores": len(jsons)}],
        description="Detalhes de senadores",
    )

    dest = Path(out_dir) / "detalhes_senadores.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
