from datetime import date
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()

TASK_NAME = "extract_proposicoes_camara"


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_proposicoes_camara(
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[int]:
    logger = get_run_logger()

    url = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}proposicoes?dataInicio={start_date}&dataFim={end_date}&itens=100&ordem=ASC&ordenarPor=id"

    logger.info("Buscando proposições da Câmara.")

    jsons = await fetch_many_jsons(
        urls=[url],
        not_downloaded_urls=[],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        validate_results=True,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    dest = Path(out_dir) / "proposicoes.ndjson"
    save_ndjson(cast(list[dict], jsons), dest)

    await acreate_table_artifact(
        key="proposicoes-camara",
        table=[generate_artifact(jsons)],
        description="Proposições da Câmara",
    )

    # OBS: ao atualizar os dados no final do dia, é possível que no meio do caminho novos dados sejam inseridos na API, o que tornará a comparação errônea pois terão mais dados sendo baixados que os contabilizados inicialmente.
    ids_proposicoes = {int(p.get("id")) for j in jsons for p in j.get("dados", [])}  # type: ignore

    return list(ids_proposicoes)


def generate_artifact(jsons: Any) -> dict:
    total_proposicoes = 0
    for j in jsons:
        downloaded_proposicoes = len(j.get("dados", []))
        total_proposicoes += downloaded_proposicoes
    return {"total_proposicoes": total_proposicoes}
