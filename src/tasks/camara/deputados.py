from pathlib import Path
from prefect import task, get_run_logger

from utils.io import fetch_json, save_json
from config.loader import load_config

APP_SETTINGS = load_config()

def deputados_url(legislatura: dict) -> str:
    id_legislatura = legislatura["dados"][0]["id"]
    return f"{APP_SETTINGS.CAMARA.REST_BASE_URL}frentes?idLegislatura={id_legislatura}"

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
def extract_deputados(legislatura: dict, out_dir: str = "data/camara") -> str:
    logger = get_run_logger()
    url = deputados_url(legislatura)
    dest = Path(out_dir) / "deputados.json"
    logger.info(f"Congresso: buscando Deputados de {url} -> {dest}")
    json = fetch_json(url)
    return save_json(json, dest)