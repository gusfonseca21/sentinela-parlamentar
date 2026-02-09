from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def detalhes_senadores_urls(senadores_ids: list[str]) -> list[str]:
    return [
        f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/{id}?v=6" for id in senadores_ids
    ]


@task(
    task_run_name="extract_detalhes_senadores",
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_detalhes_senadores(
    ids_senadores: list[str],
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = detalhes_senadores_urls(ids_senadores)

    logger.info(f"Baixando detalhes de {len(urls)} senadores")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        logger=logger,
        validate_results=False,
    )

    await acreate_table_artifact(
        key="detalhes-senadores",
        table=[{"num_senadores": len(jsons)}],
        description="Detalhes de senadores",
    )

    dest = Path(out_dir) / "detalhes_senadores.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
