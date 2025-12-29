from datetime import date, datetime, timedelta
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_results
from prefect.task_runners import ThreadPoolTaskRunner

from config.loader import load_config
from config.parameters import TasksNames
from tasks import camara
from tasks.tse import TSE_ENDPOINTS, extract_tse

APP_SETTINGS = load_config()


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=APP_SETTINGS.FLOW.MAX_RUNNERS),  # type: ignore
    log_prints=True,
)
async def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
    ignore_tasks: list[str] = [],
):
    logger = get_run_logger()
    logger.info("Iniciando pipeline")

    # TSE: ~30 endpoints em paralelo
    tse_fs = [
        cast(Any, extract_tse)
        .with_options(refresh_cache=refresh_cache)
        .submit(name, url)
        for name, url in TSE_ENDPOINTS.items()
    ]

    # CÂMARA DOS DEPUTADOS

    ## EXTRACT LEGISLATURA
    legislatura_f = None
    if TasksNames.EXTRACT_CAMARA_LEGISLATURA not in ignore_tasks:
        legislatura_f = camara.extract_legislatura(start_date, end_date)

    ## EXTRACT DEPUTADOS
    deputados_f = None
    if legislatura_f and TasksNames.EXTRACT_CAMARA_DEPUTADOS not in ignore_tasks:
        deputados_f = camara.extract_deputados.submit(legislatura_f)

    ## EXTRACT ASSIDUIDADE
    assiduidade_f = None
    if deputados_f and TasksNames.EXTRACT_CAMARA_ASSIDUIDADE not in ignore_tasks:
        resolve_futures_to_results([deputados_f])
        assiduidade_f = camara.extract_assiduidade_deputados.submit(
            cast(list[int], deputados_f), start_date, end_date
        )

    ## EXTRACT FRENTES
    frentes_f = None
    if legislatura_f and TasksNames.EXTRACT_CAMARA_FRENTES not in ignore_tasks:
        id_legislatura = legislatura_f["dados"][0]["id"]
        frentes_f = camara.extract_frentes.submit(id_legislatura)

    ## EXTRACT FRENTES MEMBROS
    frentes_membros_f = None
    if frentes_f and TasksNames.EXTRACT_CAMARA_FRENTES_MEMBROS not in ignore_tasks:
        frentes_membros_f = camara.extract_frentes_membros.submit(cast(Any, frentes_f))

    ## EXTRACT DETALHES DEPUTADOOS
    detalhes_deputados_f = None
    if (
        frentes_membros_f
        and TasksNames.EXTRACT_CAMARA_DETALHES_DEPUTADOS not in ignore_tasks
    ):
        resolve_futures_to_results(frentes_membros_f)
        detalhes_deputados_f = camara.extract_detalhes_deputados.submit(
            cast(list[int], deputados_f)
        )

    ## EXTRACT DISCURSOS DEPUTADOS
    discursos_deputados_f = None
    if (
        deputados_f
        and TasksNames.EXTRACT_CAMARA_DISCURSOS_DEPUTADOS not in ignore_tasks
    ):
        resolve_futures_to_results(detalhes_deputados_f)
        discursos_deputados_f = camara.extract_discursos_deputados.submit(
            cast(list[int], deputados_f), start_date, end_date
        )

    ## EXTRACT PROPOSIÇÕES CÂMARA
    proposicoes_camara_f = None
    if TasksNames.EXTRACT_CAMARA_PROPOSICOES not in ignore_tasks:
        resolve_futures_to_results(discursos_deputados_f)
        proposicoes_camara_f = camara.extract_proposicoes_camara.submit(
            start_date, end_date
        )

    ## EXTRACT DETALHES PROPOSIÇÕES CÂMARA
    detalhes_proposicoes_camara_f = None
    if (
        proposicoes_camara_f
        and TasksNames.EXTRACT_CAMARA_DETALHES_PROPOSICOES not in ignore_tasks
    ):
        resolve_futures_to_results([proposicoes_camara_f])
        detalhes_proposicoes_camara_f = (
            camara.extract_detalhes_proposicoes_camara.submit(
                cast(list[int], proposicoes_camara_f)
            )
        )

    ## EXTRACT AUTORES PROPOSIÇÕES CÂMARA
    autores_proposicoes_camara_f = None
    if (
        proposicoes_camara_f
        and TasksNames.EXTRACT_CAMARA_AUTORES_PROPOSICOES not in ignore_tasks
    ):
        resolve_futures_to_results(detalhes_proposicoes_camara_f)
        autores_proposicoes_camara_f = camara.extract_autores_proposicoes_camara.submit(
            cast(list[int], proposicoes_camara_f)
        )

    # EXTRACT DESPESAS DEPUTADOS
    # BUGADO ""Parâmetro(s) inválido(s).""
    despesas_deputados_f = None
    if (
        legislatura_f
        and TasksNames.EXTRACT_CAMARA_DESPESAS_DEPUTADOS not in ignore_tasks
    ):
        resolve_futures_to_results([discursos_deputados_f])
        despesas_deputados_f = camara.extract_despesas_deputados.submit(
            cast(list[int], deputados_f), start_date, legislatura_f
        )

    return resolve_futures_to_results(
        {
            "tse": tse_fs,
            "congresso_deputados": deputados_f,
            "congresso_assiduidade": assiduidade_f,
            "congresso_frentes": frentes_f,
            "congresso_frentes_membros": frentes_membros_f,
            "congresso_detalhes_deputados": detalhes_deputados_f,
            "congresso_discurso_deputados": discursos_deputados_f,
            "congresso_proposicoes": proposicoes_camara_f,
            "congresso_detalhes_proposicoes": detalhes_proposicoes_camara_f,
            "autores_proposicoes_camara_fs": autores_proposicoes_camara_f,
            "congresso_despesas_deputados": despesas_deputados_f,
        }
    )


if __name__ == "__main__":
    pipeline.serve(  # type: ignore
        name="deploy-1"
    )
