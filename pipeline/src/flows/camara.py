from datetime import date

from prefect import flow, get_run_logger, task
from prefect.futures import resolve_futures_to_results
from prefect.runtime import flow_run

from config.parameters import FlowsNames, TasksNames
from tasks.extract.camara import (
    extract_assiduidade_camara,
    extract_autores_proposicoes_camara,
    extract_deputados_camara,
    extract_despesas_camara,
    extract_detalhes_deputados_camara,
    extract_detalhes_proposicoes_camara,
    extract_detalhes_votacoes_camara,
    extract_discursos_deputados_camara,
    extract_frentes_camara,
    extract_frentes_membros_camara,
    extract_legislatura,
    extract_orientacoes_votacoes_camara,
    extract_proposicoes_camara,
    extract_votacoes_camara,
    extract_votos_votacoes_camara,
)
from utils.logs import save_logs


@flow(
    name="Câmara Flow",
    flow_run_name="camara_flow",
    description="Orquestramento de tasks do endpoint Câmara.",
    log_prints=True,
)
def camara_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id: int
):
    logger = get_run_logger()
    logger.info(f"Iniciando execução da Flow da Câmara - Lote {lote_id}")

    ## LEGISLATURA
    extract_camara_legislatura_f = None
    if TasksNames.EXTRACT_CAMARA_LEGISLATURA not in ignore_tasks:
        extract_camara_legislatura_f = extract_legislatura(
            start_date=start_date, lote_id=lote_id
        )

    ## DEPUTADOS
    extract_camara_deputados_f = None
    if (
        extract_camara_legislatura_f is not None
        and TasksNames.EXTRACT_CAMARA_DEPUTADOS not in ignore_tasks
    ):
        extract_camara_deputados_f = extract_deputados_camara(
            legislatura=extract_camara_legislatura_f, lote_id=lote_id
        )

    ## ASSIDUIDADE
    extract_camara_assiduidade_f = None
    if (
        extract_camara_deputados_f is not None
        and TasksNames.EXTRACT_CAMARA_ASSIDUIDADE not in ignore_tasks
    ):
        extract_camara_assiduidade_f = extract_assiduidade_camara.submit(
            deputados_ids=extract_camara_deputados_f,
            start_date=start_date,
            end_date=end_date,
            lote_id=lote_id,
        )

    ## FRENTES
    extract_camara_frentes_f = None
    if (
        extract_camara_legislatura_f is not None
        and TasksNames.EXTRACT_CAMARA_FRENTES not in ignore_tasks
    ):
        extract_camara_frentes_f = extract_frentes_camara.submit(
            legislatura=extract_camara_legislatura_f, lote_id=lote_id
        )
        resolve_futures_to_results(extract_camara_frentes_f)

    ## FRENTES MEMBROS
    extract_camara_frentes_membros_f = None
    if (
        extract_camara_frentes_f is not None
        and TasksNames.EXTRACT_CAMARA_FRENTES_MEMBROS not in ignore_tasks
    ):
        extract_camara_frentes_membros_f = extract_frentes_membros_camara.submit(
            frentes_ids=extract_camara_frentes_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_camara_frentes_membros_f)

    ## DETALHES DEPUTADOS
    extract_camara_detalhes_deputados_f = None
    if (
        extract_camara_deputados_f is not None
        and TasksNames.EXTRACT_CAMARA_DETALHES_DEPUTADOS not in ignore_tasks
    ):
        extract_camara_detalhes_deputados_f = extract_detalhes_deputados_camara.submit(
            deputados_ids=extract_camara_deputados_f, lote_id=lote_id
        )
        resolve_futures_to_results(extract_camara_detalhes_deputados_f)

    ## DISCURSOS DEPUTADOS
    extract_camara_discursos_deputados_f = None
    if (
        extract_camara_deputados_f is not None
        and TasksNames.EXTRACT_CAMARA_DISCURSOS_DEPUTADOS not in ignore_tasks
    ):
        extract_camara_discursos_deputados_f = (
            extract_discursos_deputados_camara.submit(
                deputados_ids=extract_camara_deputados_f,
                start_date=start_date,
                end_date=end_date,
                lote_id=lote_id,
            )
        )
        resolve_futures_to_results(extract_camara_discursos_deputados_f)

    ## PROPOSIÇÕES
    extract_camara_proposicoes_f = None
    if TasksNames.EXTRACT_CAMARA_PROPOSICOES not in ignore_tasks:
        extract_camara_proposicoes_f = extract_proposicoes_camara.submit(
            start_date=start_date, end_date=end_date, lote_id=lote_id
        )
        resolve_futures_to_results(extract_camara_proposicoes_f)

    ## DETALHES PROPOSIÇÕES
    extract_camara_detalhes_proposicoes_f = None
    if (
        extract_camara_proposicoes_f is not None
        and TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES not in ignore_tasks
    ):
        extract_camara_detalhes_proposicoes_f = (
            extract_detalhes_proposicoes_camara.submit(
                proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
                lote_id=lote_id,
            )
        )
        resolve_futures_to_results(extract_camara_detalhes_proposicoes_f)

    ## AUTORES PROPOSIÇÕES
    extract_camara_autores_proposicoes_f = None
    if (
        extract_camara_proposicoes_f is not None
        and TasksNames.EXTRACT_CAMARA_AUTORES_PROPOSICOES not in ignore_tasks
    ):
        extract_camara_autores_proposicoes_f = (
            extract_autores_proposicoes_camara.submit(
                proposicoes_ids=extract_camara_proposicoes_f,  # type: ignore
                lote_id=lote_id,
            )
        )
        resolve_futures_to_results(extract_camara_autores_proposicoes_f)

    ## VOTAÇÕES CÂMARA
    extract_camara_votacoes_f = None
    if TasksNames.EXTRACT_CAMARA_VOTACOES not in ignore_tasks:
        extract_camara_votacoes_f = extract_votacoes_camara.submit(
            start_date=start_date, end_date=end_date, lote_id=lote_id
        )
        resolve_futures_to_results(extract_camara_votacoes_f)

    ## DETALHES VOTAÇÕES
    extract_camara_detalhes_votacoes_f = None
    if (
        extract_camara_votacoes_f is not None
        and TasksNames.EXTRACT_CAMARA_DETALHES_VOTACOES not in ignore_tasks
    ):
        extract_camara_detalhes_votacoes_f = extract_detalhes_votacoes_camara.submit(
            votacoes_ids=extract_camara_votacoes_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_camara_detalhes_votacoes_f)

    ## ORIENTAÇÕES VOTAÇÕES
    extract_camara_orientacoes_votacoes_f = None
    if (
        extract_camara_votacoes_f is not None
        and TasksNames.EXTRACT_CAMARA_ORIENTACOES_VOTACOES not in ignore_tasks
    ):
        extract_camara_orientacoes_votacoes_f = (
            extract_orientacoes_votacoes_camara.submit(
                votacoes_ids=extract_camara_votacoes_f,  # type: ignore
                lote_id=lote_id,
            )
        )
        resolve_futures_to_results(extract_camara_orientacoes_votacoes_f)

    ## VOTOS VOTAÇÕES CÂMARA
    extract_camara_votos_votacoes_f = None
    if (
        extract_camara_votacoes_f is not None
        and TasksNames.EXTRACT_CAMARA_VOTOS_VOTACOES not in ignore_tasks
    ):
        extract_camara_votos_votacoes_f = extract_votos_votacoes_camara.submit(
            votacoes_ids=extract_camara_votacoes_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_camara_votos_votacoes_f)

    ## DESPESAS DEPUTADOS
    # BUGADO ""Parâmetro(s) inválido(s).""
    extract_camara_despesas_deputados_f = None
    if (
        extract_camara_legislatura_f is not None
        and TasksNames.EXTRACT_CAMARA_DESPESAS_DEPUTADOS not in ignore_tasks
    ):
        extract_camara_despesas_deputados_f = extract_despesas_camara.submit(
            deputados_ids=extract_camara_deputados_f,  # type: ignore
            start_date=start_date,
            end_date=end_date,
            legislatura=extract_camara_legislatura_f,  # type: ignore
            lote_id=lote_id,
        )
        resolve_futures_to_results(extract_camara_despesas_deputados_f)

    resolve_futures_to_results(extract_camara_assiduidade_f)

    save_logs(
        flow_run_name=FlowsNames.CAMARA.value,
        flow_run_id=flow_run.id,
        lote_id=lote_id,
    )

    return


@task(
    name="Run Câmara Flow",
    task_run_name="run_camara_flow",
    description="Task que permite executar o Flow da Câmara de forma concorrente em relação às outras flows.",
)
def run_camara_flow(
    start_date: date, end_date: date, ignore_tasks: list[str], lote_id: int
):
    camara_flow(start_date, end_date, ignore_tasks, lote_id)
