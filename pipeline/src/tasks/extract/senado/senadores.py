from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import create_table_artifact

from config.loader import load_config
from utils.io import fetch_json, save_json

APP_SETTINGS = load_config()


@task(
    task_run_name="extract_senadores",
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
def extract_senadores(
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
) -> list[str]:
    logger = get_run_logger()

    url_exerc = f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/lista/atual?v=4"
    url_afast = f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/afastados"

    logger.info(f"Baixando Senadores em exercício: {url_exerc}")
    logger.info(f"Baixando Senadores afastados: {url_afast}")

    dest_exerc = Path(out_dir) / "senadores_exercicio.json"
    dest_afast = Path(out_dir) / "senadores_afastados.json"

    json_exerc = fetch_json(
        url=url_exerc, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )
    json_afast = fetch_json(
        url=url_afast, max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES
    )

    json_exerc = cast(dict, json_exerc)
    json_afast = cast(dict, json_afast)

    save_json(json_exerc, dest_exerc)
    save_json(json_afast, dest_afast)

    ids_sens_exerc = {
        senador.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", "")
        for senador in json_exerc.get("ListaParlamentarEmExercicio", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    }

    ids_sens_afast = {
        senador.get("IdentificacaoParlamentar", {}).get("CodigoParlamentar", "")
        for senador in json_afast.get("AfastamentoAtual", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    }

    ids_senadores = list(ids_sens_exerc | ids_sens_afast)

    create_table_artifact(
        key="senadores",
        table=generate_artifact(json_exerc, json_afast),
        description="Senadores que atuaram na Legislatura",
    )

    return ids_senadores


def generate_artifact(json_exerc: dict, json_afast: dict):
    artifact_data = []

    sens_exerc = (
        json_exerc.get("ListaParlamentarEmExercicio", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    )
    sens_afast = (
        json_afast.get("AfastamentoAtual", {})
        .get("Parlamentares", {})
        .get("Parlamentar", [])
    )

    for i, senador in enumerate(sens_exerc):
        ident = senador.get("IdentificacaoParlamentar", {})
        artifact_data.append(
            {
                "index": i,
                "id": ident.get("CodigoParlamentar", ""),
                "nome": ident.get("NomeParlamentar", ""),
                "partido": ident.get("SiglaPartidoParlamentar", ""),
                "uf": ident.get("UfParlamentar", ""),
                "exercicio": "Sim",
            }
        )

    for i, senador in enumerate(sens_afast):
        ident = senador.get("IdentificacaoParlamentar", {})
        artifact_data.append(
            {
                "index": i,
                "id": ident.get("CodigoParlamentar", ""),
                "nome": ident.get("NomeParlamentar", ""),
                "partido": ident.get("SiglaPartidoParlamentar", ""),
                "exercicio": "Não",
            }
        )

    return artifact_data
