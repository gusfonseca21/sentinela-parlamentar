from datetime import date, timedelta
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_camara import fetch_many_camara
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def urls_despesas(
    deputados_ids: list[int], start_date: date, legislatura: dict
) -> list[str]:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    today = date.today()
    # Se start_date for menor que o ano atual, irá baixar todos os dados de despesas
    if start_date.year < today.year:
        return [
            f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/despesas?idLegislatura={id_legislatura}&itens=80"
            for id in deputados_ids
        ]

    else:
        # O Deputado tem 3 meses para apresentar a nota
        curr_month = today.month
        three_months_back = today - timedelta(days=90)
        three_months_urls = set()
        for id in deputados_ids:
            for month in range(three_months_back.month, curr_month + 1):
                three_months_urls.add(
                    f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/despesas?ano={today.year}&mes={month}&itens=80&ordem=ASC"
                )

        return list(three_months_urls)


@task(
    task_run_name="extract_despesas_deputados",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_despesas_deputados(
    deputados_ids: list[int],
    start_date: date,
    legislatura: dict,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUT_DIR,
) -> str:
    logger = get_run_logger()

    urls = urls_despesas(deputados_ids, start_date, legislatura)
    logger.info(f"Câmara: buscando despesas de {len(urls)} URLs")

    jsons = await fetch_many_camara(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        validate_results=True,
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
