from pathlib import Path
from prefect import task, get_run_logger
from prefect.artifacts import acreate_table_artifact
from typing import cast
from urllib.parse import urlparse, parse_qs
from datetime import date

from utils.io import fetch_json_many_async, save_ndjson
from utils.url import get_path_parameter_value
from config.loader import load_config

APP_SETTINGS = load_config()

def urls_discursos(deputados_ids: list[int], start_date: date) -> list[str]:
    return [f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}/discursos?dataInicio={start_date}&itens=1000" for id in deputados_ids]

@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT
)
async def extract_discursos_deputados(deputados_ids: list[int], start_date: date, out_dir: str | Path = "data/camara") -> str:
    logger = get_run_logger()

    urls = urls_discursos(deputados_ids, start_date)
    logger.info(f"Câmara: buscando discursos de {len(urls)} deputados")

    jsons = await fetch_json_many_async(
        urls=urls,
        concurrency=APP_SETTINGS.CAMARA.CONCURRENCY,
        timeout=APP_SETTINGS.CAMARA.TIMEOUT,
        follow_pagination=True
    )

    # Gerando artefato para validação dos dados
    artifact_data = []
    for i, json in enumerate(jsons):
        json = cast(dict, json)
        discursos = json.get("dados", []) # type: ignore
        links = {l["rel"]: l["href"] for l in json["links"]}

        # Pegando o id do deputado
        deputado_id = get_path_parameter_value(url=links.get("self", ""), param_name="deputados")

        # Aqui next é usado pois não precisa varrer a lista inteira, ele para no primeiro que encontrar
        row = next((row for row in artifact_data if row["id"] == deputado_id), None)

        if row: # Se já tiver um registro, atualiza o número de discursos
            row["num_discursos"] += len(discursos)
        else: # Se não, cria novo registro
            artifact_data.append({
                "index": i,
                "id": deputado_id,
                "num_discursos": len(discursos)
            })

    await acreate_table_artifact(
        key="discursos-deputados",
        table=artifact_data,
        description="Discursos de deputados"
    )

    dest = Path(out_dir) / "discursos.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)