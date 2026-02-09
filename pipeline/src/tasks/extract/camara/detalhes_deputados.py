from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def detalhes_deputados_urls(deputados_ids: list[int]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}" for id in deputados_ids
    ]


@task(
    task_run_name="extract_detalhes_deputados",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_detalhes_deputados(
    deputados_ids: list[int],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = detalhes_deputados_urls(deputados_ids)
    logger.info(f"Câmara: baixando dados de {len(urls)} Deputado")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        logger=logger,
        validate_results=True,
    )

    await acreate_table_artifact(
        key="detalhes-deputados",
        table=generate_artifact(jsons),
        description="Detalhes de deputados",
    )

    dest = Path(out_dir) / "detalhes_deputados.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    artifact_data = []
    for i, json in enumerate(jsons):
        json = cast(dict, json)
        deputado = json.get("dados", [])  # type: ignore
        artifact_data.append(
            {
                "index": i,
                "id": deputado.get("id", None),
                "nome": deputado.get("ultimoStatus", {}).get("nome", None),
                "situacao": deputado.get("ultimoStatus", {}).get("situacao", None),
                "condicao_eleitoral": deputado.get("ultimoStatus", {}).get(
                    "condicaoEleitoral", None
                ),
            }
        )
    return artifact_data
