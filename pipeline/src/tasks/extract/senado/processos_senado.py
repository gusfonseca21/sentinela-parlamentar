from datetime import date
from pathlib import Path
from typing import Any

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_json

APP_SETTINGS = load_config()

TASK_NAME = "extract_processos_senado"


def get_processos_url(start_date: date, end_date: date, logger: Any) -> list[str]:
    date_dif = end_date - start_date

    dif_days = date_dif.days

    if dif_days > 30:
        logger.warning(
            "Para download de Processos do Senado as datas têm mais de 30 dias de diferença, serão baixadas todas as proposições que entraram em tramitação na atual legislatura"
        )
        # Se for maior que 30 dias, baixa todos os Processos que tramitaram na Legislatura
        return [
            f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo?tramitouLegislaturaAtual=S"
        ]
    else:
        return [
            f"{APP_SETTINGS.SENADO.REST_BASE_URL}processo?tramitouLegislaturaAtual=S&numdias={dif_days}&v=1"
        ]


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_processos_senado(
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    url = get_processos_url(start_date, end_date, logger)

    logger.info(f"Baixando Processos do Senado: {url}")

    json = await fetch_many_jsons(
        urls=url,
        not_downloaded_urls=[],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="processos-senado",
        table=[{"num_processos": len(json[0])}],
        description="Proposições Senado",
    )

    dest = Path(out_dir) / "processos.json"

    save_json(json, dest)

    ids = get_processos_ids(json)

    return ids


def get_processos_ids(json: Any) -> list[str]:
    if json is None:
        raise ValueError("O dict json é inválido")

    processos = json[0]

    ids = [str(processo.get("id")) for processo in processos]

    return ids
