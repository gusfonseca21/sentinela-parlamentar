from datetime import date

from prefect import flow, get_run_logger, task
from prefect.futures import resolve_futures_to_results

from config.parameters import TasksNames
from tasks.extract.tse import (
    extract_candidatos,
    extract_prestacao_contas,
    extract_redes_sociais,
    extract_votacao,
)
from utils.br_data import BR_UFS, get_election_years


@flow(
    name="TSE Flow",
    flow_run_name="tse_flow",
    description="Orquestramento de tasks do endpoint TSE.",
    log_prints=True,
)
def tse_flow(start_date: date, refresh_cache: bool, ignore_tasks: list[str]):
    logger = get_run_logger()
    logger.info("Iniciando execução da Flow do TSE")

    elections_years = get_election_years(start_date.year)

    # EXTRACT CANDIDATOS
    extract_candidatos_f = None
    if TasksNames.EXTRACT_TSE_CANDIDATOS not in ignore_tasks:
        extract_candidatos_f = [
            extract_candidatos.with_options(refresh_cache=refresh_cache).submit(year)
            for year in elections_years
        ]  # Retorna lista de futures, que quando resolvidos retorna lista de strings

    # EXTRACT PRESTAÇÃO DE CONTAS
    extract_prestacao_contas_f = None
    if TasksNames.EXTRACT_TSE_PRESTACAO_CONTAS not in ignore_tasks:
        extract_prestacao_contas_f = [
            extract_prestacao_contas.with_options(refresh_cache=refresh_cache).submit(
                year
            )
            for year in elections_years
        ]

    # EXTRACT REDES SOCIAIS
    extract_redes_sociais_f = None
    if TasksNames.EXTRACT_TSE_REDES_SOCIAIS not in ignore_tasks:
        extract_redes_sociais_f = [
            extract_redes_sociais.with_options(refresh_cache=refresh_cache).submit(
                year, uf
            )
            for year in elections_years
            for uf in BR_UFS
            if not (uf == "DF" and year == 2018)
        ]

    # EXTRACT VOTACAO
    extract_votacao_f = None
    if TasksNames.EXTRACT_TSE_VOTACAO not in ignore_tasks:
        extract_votacao_f = [
            extract_votacao.with_options(refresh_cache=refresh_cache).submit(year)
            for year in elections_years
        ]

    # Results só é necessário para os úlitmos resultados das últimas tasks, que não são chamadas por nenhuma outra task, para finalizar o processo corretamente.
    results = resolve_futures_to_results(
        [
            extract_candidatos_f,
            extract_prestacao_contas_f,
            extract_redes_sociais_f,
            extract_votacao_f,
        ]
    )

    return results


@task(
    name="Run TSE Flow",
    task_run_name="run_tse_flow",
    description="Task que permite executar o Flow do TSE de forma concorrente em relação às outras flows.",
)
def run_tse_flow(start_date: date, refresh_cache: bool, ignore_tasks: list[str]):
    tse_flow(start_date, refresh_cache, ignore_tasks)
