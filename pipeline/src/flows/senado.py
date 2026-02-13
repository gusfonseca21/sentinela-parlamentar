from datetime import date

from prefect import flow, get_run_logger, task
from prefect.futures import resolve_futures_to_results

from config.parameters import TasksNames
from tasks.extract.senado import (
    extract_colegiados,
    extract_despesas_senado,
    extract_detalhes_processos_senado,
    extract_detalhes_senadores_senado,
    extract_discursos_senado,
    extract_processos_senado,
    extract_senadores_senado,
    extract_votacoes_senado,
)


@flow(
    name="Senado Flow",
    flow_run_name="senado_flow",
    description="Orquestramento de tasks do endpoint Senado.",
    log_prints=True,
)
def senado_flow(start_date: date, end_date: date, ignore_tasks: list[str], lote_id):
    logger = get_run_logger()
    logger.info("Iniciando execução da Flow do Senado")

    ## COLEGIADOS
    extract_colegiados_senado_f = None
    if TasksNames.EXTRACT_COLEGIADOS_SENADO not in ignore_tasks:
        extract_colegiados_senado_f = extract_colegiados.submit(lote_id=lote_id)

    ## SENADORES
    extract_senadores_senado_f = None
    if TasksNames.EXTRACT_SENADO_SENADORES not in ignore_tasks:
        extract_senadores_senado_f = extract_senadores_senado.submit(lote_id=lote_id)

    ## DETALHES SENADORES
    extract_detalhes_senadores_senado_f = None
    if (
        extract_senadores_senado_f is not None
        and TasksNames.EXTRACT_SENADO_DETALHES_SENADORES not in ignore_tasks
    ):
        extract_detalhes_senadores_senado_f = extract_detalhes_senadores_senado.submit(
            ids_senadores=extract_senadores_senado_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_detalhes_senadores_senado_f)

    ## DISCURSOS SENADORES
    extract_discursos_senado_f = None
    if (
        extract_senadores_senado_f is not None
        and TasksNames.EXTRACT_SENADO_DISCURSOS_SENADORES not in ignore_tasks
    ):
        extract_discursos_senado_f = extract_discursos_senado.submit(
            ids_senadores=extract_senadores_senado_f,  # type: ignore
            start_date=start_date,
            end_date=end_date,
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_discursos_senado_f)

    ## DESPESAS SENADORES
    extract_despesas_senado_f = None
    if TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES not in ignore_tasks:
        extract_despesas_senado_f = extract_despesas_senado.submit(
            start_date=start_date, end_date=end_date, lote_id=lote_id
        )

    ## PROCESSOS SENADO
    extract_processos_senado_f = None
    if TasksNames.EXTRACT_SENADO_PROCESSOS not in ignore_tasks:
        extract_processos_senado_f = extract_processos_senado.submit(
            start_date=start_date, end_date=end_date, lote_id=lote_id
        )

    ## DETALHES PROCESSOS
    extract_detalhes_processos_senado_f = None
    if (
        extract_processos_senado_f is not None
        and TasksNames.EXTRACT_SENADO_DETALHES_PROCESSOS not in ignore_tasks
    ):
        extract_detalhes_processos_senado_f = extract_detalhes_processos_senado.submit(
            ids_processos=extract_processos_senado_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_detalhes_processos_senado_f)

    ## VOTACOES
    extract_votacoes_senado_f = None
    if TasksNames.EXTRACT_SENADO_VOTACOES not in ignore_tasks:
        extract_votacoes_senado_f = extract_votacoes_senado.submit(
            start_date=start_date, end_date=end_date, lote_id=lote_id
        )

    # Para finalizar o Flow corretamente na GUI do servidor, é preciso resolver os futures dos endpoints que não foram passados para outras tasks.
    resolve_futures_to_results(
        [
            extract_colegiados_senado_f,
            extract_despesas_senado_f,
            extract_votacoes_senado_f,
        ]
    )


@task(
    name="Run Senado Flow",
    task_run_name="run_senado_flow",
    description="Task que permite executar o Flow do Senado de forma concorrente em relação às outras flows.",
)
def run_senado_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id: int
):
    senado_flow(start_date, end_date, ignore_tasks, lote_id)
