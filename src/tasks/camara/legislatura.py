from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import create_table_artifact
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
def extract_legislatura(start_date: date, end_date: date, out_dir: str = "data/camara") -> dict:
    LEGISLATURA_URL = f"{APP_SETTINGS.CAMARA.REST_BASE_URL}/legislaturas?data={start_date}"

    dest = Path(out_dir) / "legislatura.json"
    logger = get_run_logger()

    logger.info(f"CÂMARA: Baixando Legislatura atual de {LEGISLATURA_URL} -> {out_dir}")

    json = fetch_json(LEGISLATURA_URL)
    json = cast(dict, json)

    # Enriquecendo os dados sobre Legislatura
    leg_start_year = int(json.get("dados", [])[0].get("dataInicio")[:4])
    end_date_year = int((end_date.isoformat())[:4])
    passed_years = [year for year in range(leg_start_year, end_date_year + 1)]
    json["dados"][0]["anosPassados"] = passed_years

    save_json(json, dest)
    json = cast(dict, json)

    dados = json.get("dados", [])[0]
    create_table_artifact(
        key="legislatura",
        table=[{
            "data": start_date.isoformat(),
            "id_legislatura": dados.get("id"),
            "data_inicio": dados.get("dataInicio"),
            "data_fim": dados.get("dataFim"),
            "anos_passados": dados.get("anosPassados"),
            "ano_atual": dados.get("anoAtual")
        }],
        description="Legislatura atual"
    )

    return json