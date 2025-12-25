from datetime import timedelta
from pathlib import Path

from prefect import get_run_logger, task

from config.loader import CACHE_POLICY_MAP, load_config
from utils.br_data import BR_UFS, calculate_election_years
from utils.file import keep_only_files
from utils.io import download_stream

APP_SETTINGS = load_config()

# MONTAR ENDPOINT REDES SOCIAIS POR ESTADO
REDES_SOCIAIS_ENDPOINTS = {
    f"redes_sociais_{year}_{uf}": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/rede_social_candidato_{year}_{uf}.zip"
    for uf in BR_UFS
    for year in calculate_election_years()
    if not (uf == "DF" and year == 2018)  # Não há esses dados para a eleição de 2018
}

TSE_ENDPOINTS = {
    "candidatos_2018": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/consulta_cand_2018.zip",
    "candidatos_2022": f"{APP_SETTINGS.TSE.BASE_URL}consulta_cand/consulta_cand_2022.zip",
    "prestaca_contas_2018": f"{APP_SETTINGS.TSE.BASE_URL}prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2018.zip",
    "prestacao_contas_2022": f"{APP_SETTINGS.TSE.BASE_URL}prestacao_contas/prestacao_de_contas_eleitorais_candidatos_2022.zip",
    "resultado_eleicao_2018": f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_2018.zip",
    "resultado_eleicao_2022": f"{APP_SETTINGS.TSE.BASE_URL}votacao_candidato_munzona/votacao_candidato_munzona_2022.zip",
}

TSE_ENDPOINTS = TSE_ENDPOINTS | REDES_SOCIAIS_ENDPOINTS


@task(
    task_run_name="extract_tse_{name}",
    retries=APP_SETTINGS.TSE.TASK_RETRIES,
    retry_delay_seconds=APP_SETTINGS.TSE.TASK_RETRY_DELAY,
    timeout_seconds=APP_SETTINGS.TSE.TASK_TIMEOUT,
    cache_policy=CACHE_POLICY_MAP[APP_SETTINGS.TSE.CACHE_POLICY],
    cache_expiration=timedelta(days=APP_SETTINGS.TSE.CACHE_EXPIRATION),
    log_prints=True,
)
def extract_tse(name: str, url: str, out_dir: str = APP_SETTINGS.TSE.OUT_DIR) -> str:
    logger = get_run_logger()

    dest = Path(out_dir) / f"{name}.zip"
    logger.info(f"Fazendo download  do endpoint TSE '{url}' -> {dest}")
    dest_path = download_stream(url, dest, unzip=True)

    keep_only_files(path=APP_SETTINGS.TSE.OUT_DIR, file_ext="csv")

    if dest_path:
        return dest_path
    else:
        return "ERRO"
