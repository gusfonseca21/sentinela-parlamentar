from datetime import timedelta
from pathlib import Path

from prefect import get_run_logger, task

from config.loader import CACHE_POLICY_MAP, load_config
from utils.io import download_stream

APP_SETTINGS = load_config()


@task(
    name="Extract TSE Votação",
    task_run_name="extract_tse_votacao_{year}",
    description="Faz o download e gravação de tabelas de resultado de votação da eleição do TSE.",
    retries=APP_SETTINGS.TSE.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.TSE.TASK_TIMEOUT,
    cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True,
)
def extract_votacao(
    year: int,
    lote_id: int,
    out_dir: Path | str = APP_SETTINGS.TSE.OUTPUT_EXTRACT_DIR,
) -> str | None:
    logger = get_run_logger()

    url = f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_{year}.zip"

    dir_dest_path = Path(out_dir) / "votacao_candidato" / str(year)

    file_dest_path = dir_dest_path / f"{year}.zip"

    logger.info(
        f"Fazendo download das tabelas de resultado de votação da eleição de {year}: {url}"
    )

    _tmp_zip_dest_path = download_stream(
        url=url,
        dest_path=file_dest_path,
        unzip=True,
        task=f"extract_tse_votacao_{year}",
        lote_id=lote_id,
    )

    return str(dir_dest_path)
