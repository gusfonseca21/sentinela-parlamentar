from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def orientacoes_votacoes_urls(votacoes_ids: list[str]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}votacoes/{id}/orientacoes"
        for id in votacoes_ids
    ]


@task(
    task_run_name="extract_orientacoes_votacoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_orientacoes_votacoes_camara(
    votacoes_ids: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = orientacoes_votacoes_urls(votacoes_ids)

    logger.info(f"Baixando orientações de votações da Câmara de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        follow_pagination=False,
        validate_results=True,
    )

    await acreate_table_artifact(
        key="orientacoes-votacoes-camara",
        table=generate_artifact(jsons),
        description="Orientações Votações da Câmara",
    )

    dest = Path(out_dir) / "orientacoes_votacoes_camara.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    num_orientacoes = 0
    for j in jsons:
        for orientacao in j.get("dados", []):
            if len(orientacao):
                num_orientacoes += 1
    return [{"total_votacoes_com_orientacao": f"{num_orientacoes}/{len(jsons)}"}]
