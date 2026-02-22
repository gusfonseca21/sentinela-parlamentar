from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from config.loader import load_config
from config.parameters import TasksNames
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name=TasksNames.EXTRACT_SENADO_COLEGIADOS,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
def extract_colegiados(
    lote_id: int,
    out_dir: str = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    url = f"{APP_SETTINGS.SENADO.REST_BASE_URL}comissao/lista/colegiados"

    logger.info(f"Baixando Colegiados do Senado: {url}")

    dest = Path(out_dir) / "colegiados.json"

    json = fetch_json(url=url, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES)

    json = cast(dict, json)

    save_json(json, dest)

    num_colegiados = len(
        json.get("ListaColegiados", {}).get("Colegiados", {}).get("Colegiado", [])
    )

    logger.info(f"Número total de colegiados do Senado: {num_colegiados}")

    create_table_artifact(
        key="colegiados-senado",
        table=[{"num_colegiados": num_colegiados}],
        description="Colegiados do Senado",
    )

    return str(dest)
