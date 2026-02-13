from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def detalhes_votacoes_urls(votacoes_ids: list[str]) -> list[str]:
    return [f"{APP_SETTINGS.CAMARA.REST_BASE_URL}votacoes/{id}" for id in votacoes_ids]


@task(
    task_run_name="extract_detalhes_votacoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_detalhes_votacoes_camara(
    votacoes_ids: list[str],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = detalhes_votacoes_urls(votacoes_ids)

    logger.info(f"Baixando detalhes de votações da Câmara de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        follow_pagination=False,
        validate_results=True,
        task="extract_detalhes_votacoes_camara",
    )

    await acreate_table_artifact(
        key="detalhes-votacoes-camara",
        table=[{"total_votacoes": len(jsons)}],
        description="Detalhes Votações da Câmara",
    )

    dest = Path(out_dir) / "detalhes_votacoes_camara.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
