from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger

from config.loader import load_config
from config.parameters import FlowsNames

from .camara import run_camara_flow
from .senado import run_senado_flow
from .tse import run_tse_flow

APP_SETTINGS = load_config()


@flow(
    name="Pipeline Flow",
    flow_run_name="pipeline_flow",
    description="Onde os outros Flows são chamados e coordenados.",
    log_prints=True,
)
def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [
        "extract_camara_frentes_membros",
        "extract_camara_detalhes_deputados",
        "extract_camara_discursos_deputados",
        "extract_camara_detalhes_proposicoes",
        "extract_camara_autores_proposicoes",
        "extract_camara_proposicoes",
        "extract_camara_votacoes",
    ],
    ignore_flows: list[str] = ["tse", "senado"],
):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline ETL")

    if FlowsNames.TSE not in ignore_flows:
        run_tse_flow.submit(start_date, refresh_cache, ignore_tasks)

    if FlowsNames.CAMARA not in ignore_flows:
        run_camara_flow.submit(start_date, end_date, ignore_tasks)

    if FlowsNames.SENADO not in ignore_flows:
        run_senado_flow.submit(start_date, end_date, ignore_tasks)
