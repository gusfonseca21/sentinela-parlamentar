from datetime import timedelta
from pathlib import Path

from prefect import get_run_logger, task

from config.loader import CACHE_POLICY_MAP, load_config
from utils.io import download_stream

APP_SETTINGS = load_config()


@task(
    name="Extract TSE Redes Sociais",
    task_run_name="extract_tse_redes_sociais_{uf}_{year}",
    description="Faz o download e gravação de tabelas de redes sociais de candidatos do TSE.",
    retries=APP_SETTINGS.TSE.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.TSE.TASK_TIMEOUT,
    cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True,
)
def extract_redes_sociais(
    year: int,
    uf: str,
    out_dir: Path | str = APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR,
) -> str | None:
    logger = get_run_logger()

    url = f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/rede_social_candidato_{year}_{uf}.zip"

    dir_dest_path = Path(out_dir) / "redes_sociais" / str(year)

    file_dest_path = dir_dest_path / f"{uf}-{year}.zip"

    logger.info(
        f"Fazendo download das tabelas de redes sociais dos candidatos do estado {uf} da eleição de {year}: {url}"
    )

    _tmp_zip_dest_path = download_stream(url, file_dest_path, unzip=True)

    return str(dir_dest_path)
