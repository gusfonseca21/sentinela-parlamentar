from pathlib import Path
from prefect import task, get_run_logger
from typing import cast

from utils.io import fetch_json_many_async, save_ndjson
from config.loader import load_config

APP_SETTINGS = load_config()

def frentes_url(legislatura: dict) -> str:
    id_legislatura = legislatura["dados"][0]["id"]
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/frentes?idLegislatura={id_legislatura}"

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
async def extract_frentes(legislatura: dict, out_dir: str | Path = "data/camara") -> list[str]:
    logger = get_run_logger()
    url = frentes_url(legislatura)
    dest = Path(out_dir) / "frentes.ndjson"
    logger.info(f"Congresso: buscando Frentes de {url} -> {dest}")
    
    jsons = await fetch_json_many_async([url])
    jsons = cast(list[dict], jsons)

    save_ndjson(jsons, dest)

    # Retornando ids das frentes
    frentes_ids = []
    for json in jsons:
        frentes = json.get("dados", [])
        for frente in frentes:
            frentes_ids.append(frente["id"])

    return frentes_ids