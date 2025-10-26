from pathlib import Path
from prefect import task, get_run_logger
from datetime import date
from typing import cast

from utils.io import fetch_json, save_json

from config.loader import load_config

APP_SETTINGS = load_config()

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
def extract_legislatura(date: date, out_dir: str = "data/camara") -> dict:
    LEGISLATURA_URL = f"https://dadosabertos.camara.leg.br/api/v2/legislaturas?data={date}"

    dest = Path(out_dir) / "legislatura.json"
    logger = get_run_logger()
    json = fetch_json(LEGISLATURA_URL)
    save_json(json, dest)
    json = cast(dict, json)
    return json