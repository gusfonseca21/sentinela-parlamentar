from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_camara import fetch_many_camara
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def frentes_membros_urls(frentes_ids: list[str]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}frentes/{id}/membros" for id in frentes_ids
    ]


@task(
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_frentes_membros(
    frentes_ids: list[str], out_dir: str | Path = "data/camara"
) -> str:
    logger = get_run_logger()

    urls = frentes_membros_urls(frentes_ids)
    logger.info(f"Câmara: buscando Membros de {len(urls)} Frentes")

    jsons = await fetch_many_camara(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        follow_pagination=True,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
    )

    await acreate_table_artifact(
        key="frentes-membros",
        table=generate_artifact(jsons),
        description="Total de membros encontrados nas frentes.",
    )

    dest = Path(out_dir) / "frentes_membros.ndjson"
    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    artifact_data = []
    for i, json in enumerate(jsons):
        json = cast(dict, json)
        link_self = next(
            link["href"] for link in json.get("links", []) if link.get("rel") == "self"
        )
        id_frente = link_self.split("/")[-2]
        membros = json.get("dados", [])  # type: ignore
        artifact_data.append(
            {"index": i, "id_frente": id_frente, "numero_membros": len(membros)}
        )
    return artifact_data
