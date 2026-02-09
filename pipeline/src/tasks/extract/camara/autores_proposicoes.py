from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()


def autores_proposicoes_urls(proposicoes_ids: list[int]) -> list[str]:
    return [
        f"https://dadosabertos.camara.leg.br/api/v2/proposicoes/{id}/autores"
        for id in proposicoes_ids
    ]


@task(
    task_run_name="extract_autores_proposicoes_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_autores_proposicoes_camara(
    proposicoes_ids: list[int],
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = autores_proposicoes_urls(proposicoes_ids)

    logger.info(f"Baixando autores de {len(urls)} proposições da Câmara")

    jsons = await fetch_many_jsons(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        logger=logger,
        follow_pagination=False,
        validate_results=True,
    )

    await acreate_table_artifact(
        key="autores-proposicoes-camara",
        table=[{"total_proposicoes": len(jsons)}],
        description="Autores Proposições da Câmara",
    )

    dest = Path(out_dir) / "autores_proposicoes_camara.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)
