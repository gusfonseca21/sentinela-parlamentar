from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson

APP_SETTINGS = load_config()

TASK_NAME = "extract_orientacoes_votacoes_camara"


def orientacoes_votacoes_urls(votacoes_ids: list[str]) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(TASK_NAME)

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    for id in votacoes_ids:
        urls.add(f"{APP_SETTINGS.CAMARA.REST_BASE_URL}votacoes/{id}/orientacoes")

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_orientacoes_votacoes_camara(
    votacoes_ids: list[str],
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    logger = get_run_logger()

    urls = orientacoes_votacoes_urls(votacoes_ids)

    logger.info(f"Baixando orientações de votações da Câmara de {len(urls)} URLs")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=True,
        task=TASK_NAME,
        lote_id=lote_id,
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
