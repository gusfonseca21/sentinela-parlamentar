from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_camara import fetch_many_camara
from utils.io import save_ndjson
from utils.url_utils import get_path_parameter_value

APP_SETTINGS = load_config()


def urls_discursos(
    deputados_ids: list[int], start_date: date, end_date: date
) -> list[str]:
    # Discursos podem demorar a ser inseridos na base de dados
    one_month_back = start_date - timedelta(days=30)
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/discursos?dataInicio={one_month_back}&dataFim={end_date}&itens=100"
        for id in deputados_ids
    ]


@task(
    task_run_name="extract_discursos_deputados",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_discursos_deputados(
    deputados_ids: list[int],
    start_date: date,
    end_date: date,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUT_DIR,
) -> str:
    logger = get_run_logger()

    urls = urls_discursos(deputados_ids, start_date, end_date)
    logger.info(f"Câmara: buscando discursos de {len(urls)} deputados")

    jsons = await fetch_many_camara(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        validate_results=True,
    )

    await acreate_table_artifact(
        key="discursos-deputados",
        table=generate_artifact(jsons),
        description="Discursos de deputados",
    )

    dest = Path(out_dir) / "discursos.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    artifact_data = []
    for i, json in enumerate(jsons):
        json = cast(dict, json)
        discursos = json.get("dados", [])  # type: ignore
        links = {link["rel"]: link["href"] for link in json["links"]}

        # Pegando o id do deputado
        deputado_id = get_path_parameter_value(
            url=links.get("self", ""), param_name="deputados"
        )

        # Aqui next é usado pois não precisa varrer a lista inteira, ele para no primeiro que encontrar
        row = next((row for row in artifact_data if row["id"] == deputado_id), None)

        if row:  # Se já tiver um registro, atualiza o número de discursos
            row["num_discursos"] += len(discursos)
        else:  # Se não, cria novo registro
            artifact_data.append(
                {"index": i, "id": deputado_id, "num_discursos": len(discursos)}
            )
    return artifact_data
