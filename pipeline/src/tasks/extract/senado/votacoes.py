from datetime import date
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson
from utils.url_utils import generate_date_urls_senado

APP_SETTINGS = load_config()


def get_votacoes_urls(start_date: date, end_date: date) -> list[str] | None:
    base_url = f"{APP_SETTINGS.SENADO.REST_BASE_URL}votacao?dataInicio=%STARTDATE%&dataFim=%ENDDATE%&v=1"

    base_urls_replaced = generate_date_urls_senado(base_url, start_date, end_date)

    if base_urls_replaced is None:
        raise

    return base_urls_replaced


@task(
    task_run_name="extract_votacoes_senado",
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_votacoes_senado(
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = get_votacoes_urls(start_date, end_date)

    if urls is None:
        raise

    logger.info(f"Baixando votações do Senado: {urls}")

    jsons = await fetch_many_jsons(
        urls=urls,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task="extract_votacoes_senado",
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="votacoes-senado",
        table=generate_artifact(jsons),
        description="Votações Senado",
    )

    dest = Path(out_dir) / "votacoes_senado.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    count = 0

    for j in jsons:
        if len(j):
            count += len(j)

    return [{"num_votacoes": count}]
