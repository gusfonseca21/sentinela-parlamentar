from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def get_detalhes_processos_url(processos_ids: list[str]) -> list[str]:
    return [
        f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo/{id}?v=1" for id in processos_ids
    ]


@task(
    task_run_name="extract_detalhes_processos",
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_detalhes_processos(
    ids_processos: list[str],
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = get_detalhes_processos_url(ids_processos)

    logger.info(f"Baixando detalhes de {len(urls)} processos")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        logger=logger,
        validate_results=False,
    )

    await acreate_table_artifact(
        key="detalhes-processos",
        table=[{"num_processos": len(jsons)}],
        description="Detalhes de Processos",
    )

    dest = Path(out_dir) / "detalhes_processos.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
