from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()

TASK_NAME = "extract_despesas_senado"


def despesas_senadores_urls(start_date: date, end_date: date) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(TASK_NAME)

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    # Os Senadores têm até 3 meses para apresentar as notas fiscais
    start_date = start_date - timedelta(days=90)

    for year in range(start_date.year, end_date.year + 1):
        # O endpoint não utiliza a URL base do Senado pois é de um domínio diferente.
        urls.add(
            f"https://adm.senado.gov.br/adm-dadosabertos/api/v1/senadores/despesas_ceaps/{year}"
        )

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_despesas_senado(
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = despesas_senadores_urls(start_date, end_date)

    logger.info(f"Baixando despesas de senadores de {len(urls)} urls")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="despesas-senadores",
        table=generate_artifact(jsons, start_date),
        description="Despesas de senadores",
    )

    dest = Path(out_dir) / "despesas_senadores.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any, start_date: date):
    counter = 0

    start_date_lookback = start_date - timedelta(days=90)

    for j in jsons:
        for despesa in j:
            if start_date.year == start_date_lookback.year:
                if int(despesa.get("mes")) >= start_date_lookback.month:
                    counter += 1
            else:
                if int(despesa.get("ano")) == start_date.year:
                    counter += 1
                else:
                    if int(despesa.get("mes")) >= start_date_lookback.month:
                        counter += 1

    return [{"total_despesas": counter}]
