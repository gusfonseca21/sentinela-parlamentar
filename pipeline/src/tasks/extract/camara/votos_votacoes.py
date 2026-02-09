from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def votos_votacoes_urls(votacoes_ids: list[str]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}votacoes/{id}/votos" for id in votacoes_ids
    ]


@task(
    task_run_name="extract_votos_votacoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_votos_votacoes_camara(
    votacoes_ids: list[str],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = votos_votacoes_urls(votacoes_ids)

    logger.info(f"Baixando votos de votações da Câmara de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        follow_pagination=False,
        validate_results=True,
    )

    await acreate_table_artifact(
        key="votos-votacoes-camara",
        table=generate_artifact(jsons),
        description="Votos Votações da Câmara",
    )

    dest = Path(out_dir) / "votos_votacoes_camara.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    num_votacoes_votos = 0
    for j in jsons:
        if j.get("dados", []):
            num_votacoes_votos += 1
    return [{"total_votacoes_com_votos": f"{num_votacoes_votos}/{len(jsons)}"}]
