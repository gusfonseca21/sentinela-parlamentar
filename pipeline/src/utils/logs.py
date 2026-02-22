import asyncio
import logging
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId, LogFilterLevel

from config.loader import load_config
from database.models.base import InsertLogDB
from database.repository.logs import insert_log_db

APP_SETTINGS = load_config()


def save_logs(flow_run_name: str, flow_run_id: UUID, lote_id: int):
    """
    Resgata os logs das flows e as grava no banco de dados.
    """
    asyncio.run(
        async_save_logs(
            flow_run_name=flow_run_name, flow_run_id=flow_run_id, lote_id=lote_id
        )
    )


async def async_save_logs(
    flow_run_name: str,
    flow_run_id: UUID,
    lote_id: int,
    quiet_required=5,
    sleep_required=1,
    timeout=90,
):
    """
    Os logs da Flow Run só ficam disponíveis depois de um tempo, é necessário esperar.
    """
    async with get_client() as client:
        # 1. Esperar logs chegarem
        num_logs_available = 0
        quiet = 0
        waited = 0
        while True:
            logs = await client.read_logs(
                log_filter=LogFilter(
                    flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                ),
                limit=1,  # só precisa saber a quantidade, não os logs em si
                offset=0,
            )
            num_logs_now = len(logs)
            if num_logs_now == num_logs_available and num_logs_now > 0:
                quiet += 1
            elif num_logs_now > num_logs_available:
                quiet = 0
            num_logs_available = num_logs_now
            if quiet >= quiet_required:
                break
            if waited >= timeout:
                break
            waited += sleep_required
            await asyncio.sleep(sleep_required)

        # 2. Paginar e buscar todos os logs
        all_logs = []
        limit = 200
        offset = 0
        while True:
            batch = await client.read_logs(
                log_filter=LogFilter(
                    flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]),
                    level=LogFilterLevel(ge_=APP_SETTINGS.FLOW.LOG_DB_LEVEL),
                ),
                limit=limit,
                offset=offset,
            )
            all_logs.extend(batch)
            if len(batch) < limit:
                break
            offset += limit

        final_logs: list[InsertLogDB] = []
        for log in all_logs:
            task_run_name = None
            if log.task_run_id:
                task_run = await client.read_task_run(log.task_run_id)
                task_run_name = task_run.name

            final_logs.append(
                InsertLogDB(
                    lote_id=lote_id,
                    timestamp=log.timestamp,
                    flow_run_name=flow_run_name,
                    task_run_name=task_run_name,
                    level=logging.getLevelName(log.level),
                    message=log.message,
                )
            )

        insert_log_db(final_logs)
