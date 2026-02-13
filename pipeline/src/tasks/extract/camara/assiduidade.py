import re
from datetime import date
from pathlib import Path
from typing import cast

from prefect import get_run_logger, task
from prefect.artifacts import acreate_table_artifact
from selectolax.parser import HTMLParser

from config.loader import load_config
from utils.io import fetch_html_many_async, save_ndjson

APP_SETTINGS = load_config()


def assiduidade_urls(
    deputados_ids: list[int], start_date: date, end_date: date
) -> list[str]:
    urls = set()
    for id in deputados_ids:
        for year in range(start_date.year, end_date.year + 1):
            urls.add(
                f"{APP_SETTINGS.CAMARA.PORTAL_BASE_URL}deputados/{id}/presenca-plenario/{year}"
            )
    return list(urls)


@task(
    task_run_name="extract_assiduidade_camara",
    retries=APP_SETTINGS.CAMARA.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.CAMARA.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.CAMARA.TASK_TIMEOUT,
)
async def extract_assiduidade_camara(
    deputados_ids: list[int],
    start_date: date,
    end_date: date,
    lote_id: int,
    out_dir: str | Path = APP_SETTINGS.CAMARA.OUTPUT_EXTRACT_DIR,
) -> str:
    """
    Baixa páginas HTML com os dados sobre a assiduidade dos Deputados
    """
    logger = get_run_logger()

    urls = assiduidade_urls(deputados_ids, start_date, end_date)
    logger.info(f"Câmara: buscando assiduidade de {len(deputados_ids)}.")

    htmls = await fetch_html_many_async(
        urls=urls,
        limit=APP_SETTINGS.CAMARA.FETCH_LIMIT,
        logger=logger,
        max_retries=APP_SETTINGS.ALLENDPOINTS.FETCH_MAX_RETRIES,
    )

    href_pattern = re.compile(r"https://www\.camara\.leg\.br/deputados/\d+")
    id_ano_pattern = r"/deputados/(?P<id>\d+)\?.*ano=(?P<ano>\d+)"

    # Montando os resultados JSON e o artefato
    artifact_data = []
    json_results = []
    for html in htmls:
        tree = HTMLParser(cast(str, html))
        all_links = tree.css("a")
        for link in all_links:
            href = link.attributes.get("href", "")
            if isinstance(href, str):
                if href_pattern.match(href):
                    match = re.search(id_ano_pattern, href)
                    if match:
                        deputado_id = int(match.group("id"))
                        year = int(match.group("ano"))

                        json_results.append(
                            {"deputado_id": deputado_id, "ano": year, "html": html}
                        )

                        tables = tree.css("table.table.table-bordered")
                        name = tree.css_first("h1.titulo-internal")
                        name_text = name.text(strip=True) if name else None
                        artifact_row = {
                            "id": deputado_id,
                            "nome": name_text,
                            "ano": year,
                        }
                        if tables:
                            artifact_row["possui_dados"] = "Sim"
                        else:
                            artifact_row["possui_dados"] = "Não"
                        artifact_data.append(artifact_row)
                    else:
                        logger.warning(
                            "Não foram encontrados dados suficientes na página HTML"
                        )
            else:
                logger.warning(f"O href {href} não é string")

    await acreate_table_artifact(
        key="assiduidade", table=artifact_data, description="Assiduidade de deputados"
    )

    dest = Path(out_dir) / "assiduidade.ndjson"
    dest_path = save_ndjson(json_results, dest)
    return dest_path
