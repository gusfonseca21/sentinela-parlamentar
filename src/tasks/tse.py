from pathlib import Path
from typing import cast
from uuid import UUID
from prefect import task, get_run_logger
from prefect.artifacts import create_progress_artifact
from datetime import timedelta

from utils.io import download_stream
from utils.br_data import BR_STATES, calculate_election_years
from config.loader import load_config, CACHE_POLICY_MAP

APP_SETTINGS = load_config()

# MONTAR ENDPOINT REDES SOCIAIS POR ESTADO
REDES_SOCIAIS_ENDPOINTS = {
    f"redes_sociais_{year}_{state}": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/rede_social_candidato_{year}_{state}.zip"
    for state in BR_STATES
    for year in calculate_election_years()
}

TSE_ENDPOINTS = {
    "candidatos_2018": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/consulta_cand_2018.zip",
    "candidatos_2022": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/consulta_cand_2022.zip",
    "prestaca_contas_2018": f"{APP_SETTINGS.TSE.BASE_URL}prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2018.zip",
    "prestacao_contas_2022": f"{APP_SETTINGS.TSE.BASE_URL}prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2022.zip",
    "resultado_eleicao_2018": f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_2018.zip",
    "resultado_eleicao_2022": f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_2022.zip"
}

TSE_ENDPOINTS = TSE_ENDPOINTS | REDES_SOCIAIS_ENDPOINTS

@task(
    task_run_name="extract_tse_{name}",
    retries=APP_SETTINGS.TSE.RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.TSE.TIMEOUT,
    cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True
)
def extract_tse(name: str, url: str, out_dir: str = "data/tse") -> str:
    logger = get_run_logger()

    progress_id = create_progress_artifact(
        progress=0.0,
        description=f"Download do arquivo {name}, do TSE"
    )

    dest = Path(out_dir) / f"{name}.zip"
    logger.info(f"Fazendo download  do endpoint TSE '{url}' -> {dest}")
    dest_path = download_stream(url, dest, unzip=True, progress_artifact_id=cast(UUID, progress_id))

    return dest_path