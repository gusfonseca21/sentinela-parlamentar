from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.io import fetch_json_many_async, save_ndjson

APP_SETTINGS = load_config()


def detalhes_deputados_urls(deputados_ids: list[int]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}deputados/{id}" for id in deputados_ids
    ]


@task(
    retries=APP_SETTINGS.CAMARA.RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TIMEOUT,
)
async def extract_detalhes_deputados(
    deputados_ids: list[int], out_dir: str | Path = "data/camara"
) -> str:
    logger = get_run_logger()

    urls = detalhes_deputados_urls(deputados_ids)
    logger.info(f"Câmara: baixando dados de {len(urls)} Deputado")

    jsons = await fetch_json_many_async(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.LIMIT,
        timeout=APP_SETTINGS.CAMARA.TIMEOUT,
        follow_pagination=True,
    )

    # Gerando artefato para validação dos dados
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

    await acreate_table_artifact(
        key="detalhes-deputados",
        table=artifact_data,
        description="Detalhes de deputados",
    )

    dest = Path(out_dir) / "detalhes_deputados.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)
