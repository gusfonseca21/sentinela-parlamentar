from datetime import date

from prefect import flow, get_run_logger, task
from prefect.futures import resolve_futures_to_results

from config.parameters import TasksNames
from tasks.extract.senado import (
    extract_colegiados,
    extract_despesas_senadores,
    extract_detalhes_processos,
    extract_detalhes_senadores,
    extract_discursos_senadores,
    extract_processos,
    extract_senadores,
    extract_votacoes,
)


@flow(
    name="Senado Flow",
    flow_run_name="senado_flow",
    description="Orquestramento de tasks do endpoint Senado.",
    log_prints=True,
)
def senado_flow(start_date: date, end_date: date, ignore_tasks: list[str]):
    logger = get_run_logger()
    logger.info("Iniciando execução da Flow do Senado")

    ## COLEGIADOS
    extract_senado_colegiados_f = None
    if TasksNames.EXTRACT_SENADO_COLEGIADOS not in ignore_tasks:
        extract_senado_colegiados_f = extract_colegiados.submit()

    ## SENADORES
    extract_senadores_f = None
    if TasksNames.EXTRACT_SENADO_SENADORES not in ignore_tasks:
        extract_senadores_f = extract_senadores.submit()

    ## DETALHES SENADORES
    extract_detalhes_senadores_f = None
    if (
        extract_senadores_f is not None
        and TasksNames.EXTRACT_SENADO_DETALHES_SENADORES not in ignore_tasks
    ):
        extract_detalhes_senadores_f = extract_detalhes_senadores.submit(
            extract_senadores_f  # type: ignore
        )
        resolve_futures_to_results(extract_detalhes_senadores_f)

    ## DISCURSOS SENADORES
    extract_discursos_senadores_f = None
    if (
        extract_senadores_f is not None
        and TasksNames.EXTRACT_SENADO_DISCURSOS_SENADORES not in ignore_tasks
    ):
        extract_discursos_senadores_f = extract_discursos_senadores.submit(
            extract_senadores_f,  # type: ignore
            start_date,
            end_date,
        )
        resolve_futures_to_results(extract_discursos_senadores_f)

    ## DESPESAS SENADORES
    extract_despesas_senadores_f = None
    if TasksNames.EXTRACT_SENADO_DESPESAS_SENADORES not in ignore_tasks:
        extract_despesas_senadores_f = extract_despesas_senadores.submit(
            start_date, end_date
        )

    ## PROCESSOS SENADO
    extract_processos_f = None
    if TasksNames.EXTRACT_SENADO_PROCESSOS not in ignore_tasks:
        extract_processos_f = extract_processos.submit(start_date, end_date)

    ## DETALHES PROCESSOS
    extract_detalhes_processos_f = None
    if (
        extract_processos_f is not None
        and TasksNames.EXTRACT_SENADO_DETALHES_PROCESSOS not in ignore_tasks
    ):
        extract_detalhes_processos_f = extract_detalhes_processos.submit(
            extract_processos_f  # type: ignore
        )
        resolve_futures_to_results(extract_detalhes_processos_f)

    ## VOTACOES
    extract_votacoes_f = None
    if TasksNames.EXTRACT_SENADO_VOTACOES not in ignore_tasks:
        extract_votacoes_f = extract_votacoes.submit(start_date, end_date)

    # Para finalizar o Flow corretamente na GUI do servidor, é preciso resolver os futures dos endpoints que não foram passados para outras tasks.
    resolve_futures_to_results(
        [extract_senado_colegiados_f, extract_despesas_senadores_f, extract_votacoes_f]
    )


@task(
    name="Run Senado Flow",
    task_run_name="run_senado_flow",
    description="Task que permite executar o Flow do Senado de forma concorrente em relação às outras flows.",
)
def run_senado_flow(start_date: date, end_date: date, ignore_tasks: list[str]):
    senado_flow(start_date, end_date, ignore_tasks)
