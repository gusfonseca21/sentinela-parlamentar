from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()

TASK_NAME = "extract_votacoes_camara"


def generate_urls(start_date: date, end_date: date) -> list[str]:
    # Documentação do endpoint diz que a dataInicio e dataFim só podem ser utilizadas se estiverem no mesmo ano.
    # Votações com dataInicio e dataFim com diferença maior que três meses retorna erro.
    current_start = start_date
    urls = []

    while current_start < end_date:
        current_end = current_start + timedelta(days=90)

        if current_end > end_date:
            current_end = end_date

        if current_end.year > current_start.year:
            current_end = date(current_start.year, 12, 31)

        urls.append(
            f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/votacoes?dataInicio={current_start}&dataFim={current_end}&itens=100"
        )

        current_start = current_end + timedelta(days=1)

    return urls


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_votacoes_camara(
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[str]:
    logger = get_run_logger()

    urls = generate_urls(start_date, end_date)

    logger.info(f"Baixando dados de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    dest = Path(out_dir) / "votacoes.ndjson"
    save_ndjson(cast(list[dict], jsons), dest)

    await acreate_table_artifact(
        key="votacoes-camara",
        table=[generate_artifact(jsons)],
        description="Votações da Câmara",
    )

    ids_votacoes = {str(p.get("id")) for j in jsons for p in j.get("dados", [])}  # type: ignore

    return list(ids_votacoes)


def generate_artifact(jsons: Any) -> dict:
    total_votacoes = 0
    for j in jsons:
        downloaded_votacoes = len(j.get("dados", []))
        total_votacoes += downloaded_votacoes
    return {"total_proposicoes": total_votacoes}
