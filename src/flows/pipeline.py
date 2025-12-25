from datetime import date, datetime, timedelta
from typing import Any, cast

from prefect import flow, get_run_logger
from prefect.futures import resolve_futures_to_results
from prefect.task_runners import ThreadPoolTaskRunner

from config.loader import load_config
from tasks import camara
from tasks.tse import TSE_ENDPOINTS, extract_tse
from utils.file import keep_only_files

APP_SETTINGS = load_config()

# IMPORTAR TASKS TSE, CONGRESSO, SENADO ETC...


@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=APP_SETTINGS.FLOW.MAX_RUNNERS),  # type: ignore
    log_prints=True,
)
async def pipeline(
    start_date: date = datetime.now().date()
    - timedelta(days=APP_SETTINGS.FLOW.DATE_LOOKBACK),
    end_date: date = datetime.now().date(),
    refresh_cache: bool = False,
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

    # # CONGRESSO
    # legislatura = camara.extract_legislatura(start_date, end_date)
    # deputados_f = camara.extract_deputados.submit(legislatura)
    # id_legislatura = legislatura["dados"][0]["id"]

    # resolve_futures_to_results([deputados_f])
    # assiduidade_fs = camara.extract_assiduidade_deputados.submit(
    #     cast(list[int], deputados_f), start_date, end_date
    # )

    # frentes_f = camara.extract_frentes.submit(id_legislatura)
    # frentes_membros_f = camara.extract_frentes_membros.submit(cast(Any, frentes_f))

    # resolve_futures_to_results(frentes_membros_f)
    # detalhes_deputados_fs = camara.extract_detalhes_deputados.submit(
    #     cast(list[int], deputados_f)
    # )

    # resolve_futures_to_results(detalhes_deputados_fs)
    # discursos_deputados_fs = camara.extract_discursos_deputados.submit(
    #     cast(list[int], deputados_f), start_date, end_date
    # )

    # resolve_futures_to_results(discursos_deputados_fs)
    # proposicoes_camara_fs = camara.extract_proposicoes_camara.submit(
    #     start_date, end_date
    # )

    # resolve_futures_to_results([proposicoes_camara_fs])
    # detalhes_proposicoes_camara_fs = camara.extract_detalhes_proposicoes_camara.submit(
    #     cast(list[int], proposicoes_camara_fs)
    # )

    # resolve_futures_to_results(detalhes_proposicoes_camara_fs)
    # autores_proposicoes_camara_fs = camara.extract_autores_proposicoes_camara.submit(
    #     cast(list[int], proposicoes_camara_fs)
    # )

    # # BUGADO ""Parâmetro(s) inválido(s).""
    # resolve_futures_to_results([discursos_deputados_fs])
    # despesas_deputados_fs = camara.extract_despesas_deputados.submit(
    #     cast(list[int], deputados_f), start_date, legislatura
    # )

    return resolve_futures_to_results(
        {
            "tse": tse_fs,
            # "congresso_deputados": deputados_f,
            # "congresso_assiduidade": assiduidade_fs,
            # "congresso_frentes": frentes_f,
            # "congresso_frentes_membros": frentes_membros_f,
            # "congresso_detalhes_deputados": detalhes_deputados_fs,
            # "congresso_discurso_deputados": discursos_deputados_fs,
            # "congresso_proposicoes": proposicoes_camara_fs,
            # "congresso_detalhes_proposicoes": detalhes_proposicoes_camara_fs,
            # "autores_proposicoes_camara_fs": autores_proposicoes_camara_fs,
            # "congresso_despesas_deputados": despesas_deputados_fs,
        }
    )


if __name__ == "__main__":
    pipeline.serve(  # type: ignore
        name="deploy-1"
    )
