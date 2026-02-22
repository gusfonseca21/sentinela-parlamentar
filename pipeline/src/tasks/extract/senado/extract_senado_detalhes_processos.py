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


def get_detalhes_processos_url(processos_ids: list[str]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(
        TasksNames.EXTRACT_SENADO_DETALHES_PROCESSOS
    )

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in processos_ids:
        urls.add(f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo/{id}?v=1")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TasksNames.EXTRACT_SENADO_DETALHES_PROCESSOS,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_detalhes_processos_senado(
    ids_processos: list[str],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = get_detalhes_processos_url(ids_processos)

    logger.info(f"Baixando detalhes de {len(urls)} URLs de Detalhes de Processos")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TasksNames.EXTRACT_SENADO_DETALHES_PROCESSOS,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="detalhes-processos",
        table=[{"num_processos": len(jsons)}],
        description="Detalhes de Processos",
    )

    dest = Path(out_dir) / "detalhes_processos.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
