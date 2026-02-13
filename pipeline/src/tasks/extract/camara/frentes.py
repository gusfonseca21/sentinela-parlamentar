from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def frentes_url(id_legislatura: int) -> str:
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/frentes?idLegislatura={id_legislatura}"


@task(
    task_run_name="extract_frentes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_frentes_camara(
    legislatura: dict,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> list[str]:
    logger = get_run_logger()

    id_legislatura = legislatura["dados"][0]["id"]

    url = frentes_url(id_legislatura)
    dest = Path(out_dir) / "frentes.ndjson"
    logger.info(f"Congresso: buscando Frentes de {url} -> {dest}")

    jsons = await fetch_many_jsons(
        urls=[url],
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=True,
        logger=logger,
        validate_results=True,
        task="extract_frentes_camara",
    )
    jsons = cast(list[dict], jsons)

    save_ndjson(jsons, dest)

    # Retornando ids das frentes
    frentes_ids = []
    artifact_data = []
    for json in jsons:
        frentes = json.get("dados", [])
        for frente in frentes:
            frentes_ids.append(frente.get("id"))
            artifact_data.append({"id": frente.get("id"), "nome": frente.get("titulo")})

    await acreate_table_artifact(
        key="frentes", table=artifact_data, description="Frentes"
    )

    return frentes_ids
