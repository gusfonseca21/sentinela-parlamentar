from datetime import date
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()

TASK_NAME = "extract_despesas_camara"


def urls_despesas(
    deputados_ids: list[int], start_date: date, end_date: date
) -> UrlsResult:
    """
    Gera URLs para cada deputado no período entre start_date e end_date.
    Utiliza o parâmetro ano como único parâmetro, se possível. Se não, ano + mês.

    Nota: start_date é automaticamente ajustado 3 meses antes para considerar
    o período de graça que deputados têm para registrar despesas.
    """
    year_offset = (start_date.month - 3 - 1) // 12
    new_month = ((start_date.month - 3 - 1) % 12) + 1
    adjusted_start = date(start_date.year + year_offset, new_month, 1)

    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(TASK_NAME)

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id_deputado in deputados_ids:
        # If the range covers full years, use year-only URLs
        if (
            adjusted_start.month == 1
            and adjusted_start.day == 1
            and end_date.month == 12
            and end_date.day == 31
        ):
            # Full year range
            for year in range(adjusted_start.year, end_date.year + 1):
                urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/deputados/{id_deputado}/despesas?"
                    f"ano={year}&ordem=ASC&ordenarPor=dataDocumento&itens=100"
                )
            continue
        # If start and end are in different years
        if adjusted_start.year < end_date.year:
            # Add months from adjusted_start to end of its year
            current = adjusted_start
            while current.year == adjusted_start.year:
                urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/deputados/{id_deputado}/despesas?"
                    f"ano={current.year}&mes={current.month}&ordem=ASC&ordenarPor=dataDocumento&itens=100"
                )
                if current.month == 12:
                    break
                current = date(current.year, current.month + 1, 1)
            # Add full years in between (if any)
            for year in range(adjusted_start.year + 1, end_date.year):
                urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/deputados/{id_deputado}/despesas?"
                    f"ano={year}&ordem=ASC&ordenarPor=dataDocumento&itens=100"
                )
            # Add months from start of end_date's year to end_date
            current = date(end_date.year, 1, 1)
            while current <= end_date:
                urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/deputados/{id_deputado}/despesas?"
                    f"ano={current.year}&mes={current.month}&ordem=ASC&ordenarPor=dataDocumento&itens=100"
                )
                if current.month == 12:
                    break
                current = date(current.year, current.month + 1, 1)
        else:
            # Same year - just iterate months
            current = adjusted_start
            while current <= end_date:
                urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/deputados/{id_deputado}/despesas?"
                    f"ano={current.year}&mes={current.month}&ordem=ASC&ordenarPor=dataDocumento&itens=100"
                )
                if current.month == 12:
                    break
                current = date(current.year, current.month + 1, 1)

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_despesas_camara(
    deputados_ids: list[int],
    start_date: date,
    end_date: date,
    legislatura: dict,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = urls_despesas(deputados_ids, start_date, end_date)
    logger.info(f"Câmara: buscando despesas de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    # Gerando artefato para validação dos dados
    artifact_data = [{"Total de registros": len(jsons)}]

    await acreate_table_artifact(
        key="despesas-deputados",
        table=artifact_data,
        description="Despesas de deputados",
    )

    dest = Path(out_dir) / "despesas.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)
