from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.io import fetch_json_many_async, save_ndjson

APP_SETTINGS = load_config()


def frentes_url(id_legislatura: int) -> str:
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/frentes?idLegislatura={id_legislatura}"


@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT,
)
async def extract_frentes(
    id_legislatura: int, out_dir: str | Path = "data/camara"
) -> list[str]:
    logger = get_run_logger()
    url = frentes_url(id_legislatura)
    dest = Path(out_dir) / "frentes.ndjson"
    logger.info(f"Congresso: buscando Frentes de {url} -> {dest}")

    jsons = await fetch_json_many_async([url])
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
