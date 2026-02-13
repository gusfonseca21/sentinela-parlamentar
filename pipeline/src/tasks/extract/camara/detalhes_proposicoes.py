from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def detalhes_proposicoes_urls(proposicoes_ids: list[int]) -> list[str]:
    return [
        f"{APP_SETTINGS.CAMARA.REST_BASE_URL}proposicoes/{id}" for id in proposicoes_ids
    ]


@task(
    task_run_name="extract_detalhes_proposicoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_detalhes_proposicoes_camara(
    proposicoes_ids: list[int],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = detalhes_proposicoes_urls(proposicoes_ids)

    logger.info(f"Baixando detalhes de {len(urls)} proposições da Câmara")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        follow_pagination=False,
        validate_results=True,
        task="extract_detalhes_proposicoes_camara",
    )

    await acreate_table_artifact(
        key="detalhes-proposicoes-camara",
        table=[{"total_proposicoes": len(jsons)}],
        description="Detalhes Proposições da Câmara",
    )

    dest = Path(out_dir) / "detalhes_proposicoes_camara.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
