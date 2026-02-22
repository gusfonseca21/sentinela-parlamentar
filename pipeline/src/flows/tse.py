from datetime import date

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

from config.parameters import FlowsNames, TasksNames
from tasks.extract.tse import (
    extract_candidatos,
    extract_prestacao_contas,
    extract_redes_sociais,
    extract_votacao,
)
from utils.br_data import BR_UFS, get_election_years
from utils.logs import save_logs


@flow(
    name="TSE Flow",
    flow_run_name="tse_flow",
    description="Orquestramento de tasks do endpoint TSE.",
    log_prints=True,
)
def tse_flow(
    start_date: date, refresh_cache: bool, ignore_tasks: list[str], lote_id: int
):
    logger = get_run_logger()
    logger.info(f"Iniciando execução da Flow do TSE - Lote {lote_id}")

    elections_years = get_election_years(start_date.year)

    futures = []

    # EXTRACT CANDIDATOS
    extract_candidatos_f = None
    if TasksNames.EXTRACT_TSE_CANDIDATOS not in ignore_tasks:
        extract_candidatos_f = [
            extract_candidatos.with_options(refresh_cache=refresh_cache).submit(
                year=year, lote_id=lote_id
            )
            for year in elections_years
        ]  # Retorna lista de futures, que quando resolvidos retorna lista de strings
        futures.extend(extract_candidatos_f)

    # EXTRACT PRESTAÇÃO DE CONTAS
    extract_prestacao_contas_f = None
    if TasksNames.EXTRACT_TSE_PRESTACAO_CONTAS not in ignore_tasks:
        extract_prestacao_contas_f = [
            extract_prestacao_contas.with_options(refresh_cache=refresh_cache).submit(
                year=year, lote_id=lote_id
            )
            for year in elections_years
        ]
        futures.extend(extract_prestacao_contas_f)

    # EXTRACT REDES SOCIAIS
    extract_redes_sociais_f = None
    if TasksNames.EXTRACT_TSE_REDES_SOCIAIS not in ignore_tasks:
        extract_redes_sociais_f = [
            extract_redes_sociais.with_options(refresh_cache=refresh_cache).submit(
                year=year, uf=uf, lote_id=lote_id
            )
            for year in elections_years
            for uf in BR_UFS
            if not (uf == "DF" and year == 2018)
        ]
        futures.extend(extract_redes_sociais_f)

    # EXTRACT VOTACAO
    extract_votacao_f = None
    if TasksNames.EXTRACT_TSE_VOTACAO not in ignore_tasks:
        extract_votacao_f = [
            extract_votacao.with_options(refresh_cache=refresh_cache).submit(
                year, lote_id
            )
            for year in elections_years
        ]
        futures.extend(extract_votacao_f)

    # Results só é necessário para os úlitmos resultados das últimas tasks, que não são chamadas por nenhuma outra task, para finalizar o processo corretamente.
    for future in futures:
        future.result()

    save_logs(
        flow_run_name=FlowsNames.TSE.value,
        flow_run_id=flow_run.id,
        lote_id=lote_id,
    )

    return


@task(
    name="Run TSE Flow",
    task_run_name="run_tse_flow",
    description="Task que permite executar o Flow do TSE de forma concorrente em relação às outras flows.",
)
def run_tse_flow(
    start_date: date, refresh_cache: bool, ignore_tasks: list[str], lote_id: int
):
    tse_flow(start_date, refresh_cache, ignore_tasks, lote_id)
