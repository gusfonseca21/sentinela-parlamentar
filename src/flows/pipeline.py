from prefect import flow, get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.futures import resolve_futures_to_results
from datetime import date, datetime
from typing import Any, cast

from tasks.tse import TSE_ENDPOINTS, extract_tse
from tasks.camara.legislatura import extract_legislatura
from tasks.camara.deputados import extract_deputados
from tasks.camara.frentes import extract_frentes
from tasks.camara.frentes_membros import extract_frentes_membros

from config.loader import load_config

APP_SETTINGS = load_config()

# IMPORTAR TASKS TSE, CONGRESSO, SENADO ETC...

@flow(
    task_runner=ThreadPoolTaskRunner(max_workers=APP_SETTINGS.FLOW.MAX_RUNNERS), # type: ignore
    log_prints=True
) 
async def pipeline(
    date: date = datetime.now().date(), 
    refresh_cache: bool = False
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

    # CONGRESSO
    legislatura = extract_legislatura(date)
    deputados_f = extract_deputados.submit(legislatura)
    frentes_f = extract_frentes.submit(legislatura)
    frentes_membros_f = extract_frentes_membros.submit(cast(Any, frentes_f))


    return resolve_futures_to_results({
        "tse": tse_fs,
        "congresso_deputados": deputados_f,
        "congresso_frentes": frentes_f,
        "congresso_frentes_membros": frentes_membros_f,
    })

if __name__ == "__main__":
    pipeline.serve( # type: ignore
        name="deploy-1"
    )