from datetime import date, datetime, timedelta

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_states
from prefect.runtime import flow_run

from config.loader import load_config
from config.parameters import FlowsNames
from database.models.base import PipelineParams
from database.repository.lote import end_lote_in_db, start_lote_in_db
from utils.logs import save_logs

from .camara import run_camara_flow
from .senado import run_senado_flow
from .tse import run_tse_flow

APP_SETTINGS = load_config()


@flow(
    name="Pipeline Flow",
    flow_run_name=FlowsNames.PIPELINE.value,
    description="Onde os outros Flows são chamados e coordenados.",
    log_prints=True,
)
def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [
        ## ----> CAMARA <----
        # "extract_camara_detalhes_deputados",
        # "extract_camara_assiduidade",
        # "extract_camara_frentes",
        # "extract_camara_frentes_membros",
        # "extract_camara_discursos_deputados",
        # "extract_camara_proposicoes",
        # "extract_camara_detalhes_proposicoes",
        # "extract_camara_autores_proposicoes",
        # "extract_camara_despesas_deputados",
        # "extract_camara_votacoes",
        # "extract_camara_detalhes_votacoes",
        # "extract_camara_orientacoes_votacoes",
        # "extract_camara_votos_votacoes",
        ## ----> SENADO <----
        # "extract_colegiados_senado",
        # "extract_senado_senadores",
        # "extract_senado_detalhes_senadores",
        # "extract_senado_discursos_senadores",
        # "extract_senado_despesas_senadores",
        # "extract_senado_processos",
        # "extract_senado_detalhes_processos",
        # "extract_senado_votacoes",
    ],
    ignore_flows: list[str] = [],
    message: str | None = None,
):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline ETL.")

    lote_id = start_lote_in_db(
        start_date_extract=start_date,
        end_date_extract=end_date,
        params=PipelineParams(
            refresh_cache=refresh_cache,
            ignore_tasks=ignore_tasks,
            ignore_flows=ignore_flows,
            message=message,
        ),
    )
    logger.info(f"Lote {lote_id} iniciou.")

    futures = []

    if FlowsNames.TSE.value not in ignore_flows:
        futures.append(
            run_tse_flow.submit(start_date, refresh_cache, ignore_tasks, lote_id)
        )

    if FlowsNames.CAMARA.value not in ignore_flows:
        futures.append(
            run_camara_flow.submit(start_date, end_date, ignore_tasks, lote_id)
        )

    if FlowsNames.SENADO.value not in ignore_flows:
        futures.append(
            run_senado_flow.submit(start_date, end_date, ignore_tasks, lote_id)
        )

    ## Bloquea a execução do código até que todos os flows sejam finalizados
    states = resolve_futures_to_states(futures)

    all_flows_ok = all(s.is_completed() for s in states)  # type:ignore

    lote_id_end = end_lote_in_db(lote_id, all_flows_ok)
    logger.info(f"Lote {lote_id_end} finalizou com sucesso")

    save_logs(
        flow_run_name=FlowsNames.PIPELINE.value,
        flow_run_id=flow_run.id,
        lote_id=lote_id,
    )

    return
