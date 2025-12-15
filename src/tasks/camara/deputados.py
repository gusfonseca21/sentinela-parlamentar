from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from config.loader import load_config
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


def deputados_url(legislatura: dict) -> str:
    id_legislatura = legislatura.get("dados", [])[0].get("id")
    return (
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados?idLegislatura={id_legislatura}"
    )


@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT,
)
def extract_deputados(legislatura: dict, out_dir: str = "data/camara") -> list[int]:
    logger = get_run_logger()
    url = deputados_url(legislatura)
    dest = Path(out_dir) / "deputados.json"
    logger.info(f"Câmara: buscando Deputados de {url} -> {dest}")
    json = fetch_json(url)
    json = cast(dict, json)

    # Gerando artefato para validação dos dados
    artifact_data = []
    for i, deputado in enumerate(json.get("dados", [])):
        artifact_data.append(
            {
                "index": i,
                "id": deputado.get("id"),
                "nome": deputado.get("nome"),
                "partido": deputado.get("siglaPartido"),
                "uf": deputado.get("siglaUf"),
            }
        )

    create_table_artifact(
        key="deputados", table=artifact_data, description="Deputados em uma Legislatura"
    )

    _dest_path = save_json(json, dest)

    ids_deputados = set()  # Retirar os ids duplicados. O JSON possui vários registros para os mesmos deputados
    ids_deputados_raw = [deputado.get("id") for deputado in json.get("dados", [])]
    ids_deputados.update(ids_deputados_raw)

    return list(ids_deputados)
