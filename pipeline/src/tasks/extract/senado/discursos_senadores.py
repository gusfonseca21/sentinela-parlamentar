from datetime import date, timedelta
from pathlib import Path
from typing import Any, cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact

from config.loader import load_config
from database.models.base import UrlsResult
from database.repository.erros_extract import verify_not_downloaded_urls_in_task_db
from utils.fetch_many_jsons import fetch_many_jsons
from utils.io import save_ndjson
from utils.url_utils import generate_date_urls_senado

APP_SETTINGS = load_config()

TASK_NAME = "extract_discursos_senado"


def discursos_senadores_urls(
    senadores_ids: list[str], start_date: date, end_date: date
) -> UrlsResult:
    urls = set()
    not_downloaded_urls = verify_not_downloaded_urls_in_task_db(TASK_NAME)

    if not_downloaded_urls:
        urls.update([error.url for error in not_downloaded_urls])

    # Baixar discursos até 1 mês atrás (podem demorar a entrarem no sistema)
    start_date = start_date - timedelta(days=30)

    base_url = f"{APP_SETTINGS.SENADO.REST_BASE_URL}senador/%ID%/discursos?dataInicio=%STARTDATE%&dataFim=%ENDDATE%&v=5"

    base_urls_replaced = generate_date_urls_senado(base_url, start_date, end_date)

    if base_urls_replaced is None:
        raise

    for url in base_urls_replaced:
        for id in senadores_ids:
            urls.add(url.replace("%ID%", id))

    return UrlsResult(
        urls_to_download=list(urls), not_downloaded_urls=not_downloaded_urls
    )


@task(
    task_run_name=TASK_NAME,
    retries=APP_SETTINGS.SENADO.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.SENADO.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.SENADO.TASK_TIMEOUT,
)
async def extract_discursos_senado(
    ids_senadores: list[str],
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.SENADO.OUTPUT_EXTRACT_DIR,
):
    logger = get_run_logger()

    urls = discursos_senadores_urls(ids_senadores, start_date, end_date)

    logger.info(f"Baixando discursos de {len(urls)} urls")

    jsons = await fetch_many_jsons(
        urls=urls["urls_to_download"],
        not_downloaded_urls=urls["not_downloaded_urls"],
        limit=APP_SETTINGS.SENADO.FETCH_LIMIT,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
        follow_pagination=False,
        validate_results=False,
        task=TASK_NAME,
        lote_id=lote_id,
    )

    await acreate_table_artifact(
        key="discursos-senadores",
        table=generate_artifact(jsons),
        description="Discursos Senadores",
    )

    dest = Path(out_dir) / "discursos_senadores.ndjson"

    return save_ndjson(cast(list[dict], jsons), dest)


def generate_artifact(jsons: Any):
    artifact_data = []

    for i, json in enumerate(jsons):
        senador = (
            json.get("DiscursosParlamentar", {})
            .get("Parlamentar", {})
            .get("IdentificacaoParlamentar")
        )

        senador_id = senador.get("CodigoParlamentar", None)

        discursos = (
            json.get("DiscursosParlamentar", {})
            .get("Parlamentar", {})
            .get("Pronunciamentos", [])
        )

        if discursos is not None:
            discursos = discursos.get("Pronunciamento", [])
        else:
            discursos = []

        if not any(item["id"] == senador_id for item in artifact_data):
            artifact_data.append(
                {
                    "index": i,
                    "id": senador_id,
                    "nome": senador.get("NomeParlamentar", None),
                    "num_discursos": len(discursos),
                }
            )
        else:
            for item in artifact_data:
                if item.get("id") == senador_id:
                    item["num_discursos"] += len(discursos)
                    break

    return artifact_data
