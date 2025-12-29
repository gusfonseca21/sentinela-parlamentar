from datetime import date
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_camara import fetch_many_camara
from utils.io import save_ndjson

APP_SETTINGS = load_config()


@task(
    task_run_name="extract_proposicoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_proposicoes_camara(
    start_date: date, end_date: date, out_dir: str | Path = APP_SETTINGS.CAMARA.OUT_DIR
) -> list[int]:
    logger = get_run_logger()

    url = f"https://dadosabertos.camara.leg.br/api/v2/proposicoes?dataInicio={start_date}&dataFim={end_date}&itens=100&ordem=ASC&ordenarPor=id"

    logger.info("Buscando proposições da Câmara.")

    jsons = await fetch_many_camara(
        urls=[url],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        validate_results=True,
    )

    dest = Path(out_dir) / "proposicoes.ndjson"
    save_ndjson(cast(list[dict], jsons), dest)

    await acreate_table_artifact(
        key="proposicoes-camara",
        table=[generate_artifact(jsons)],
        description="Proposições da Câmara",
    )

    ids_proposicoes = {int(p.get("id")) for j in jsons for p in j.get("dados", [])}  # type: ignore

    return list(ids_proposicoes)


def generate_artifact(jsons: Any) -> dict:
    total_proposicoes = 0
    for j in jsons:
        downloaded_proposicoes = len(j.get("dados", []))
        total_proposicoes += downloaded_proposicoes
    return {"total_proposicoes": total_proposicoes}
