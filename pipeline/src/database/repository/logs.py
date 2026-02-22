from sqlalchemy import insert

from database.engine import get_connection
from database.models.base import InsertLogDB, Logs

logs_t = Logs.__table__


def insert_log_db(logs: list[InsertLogDB]):
    """
    Insere no banco de dados uma lista de Logs na tabela Logs.
    """
    if not logs:
        return

    with get_connection() as conn:
        stmt = insert(logs_t).values(
            [
                {
                    "lote_id": log.lote_id,
                    "data_hora": log.timestamp,
                    "level": log.level,
                    "flow_run_name": log.flow_run_name,
                    "task_run_name": log.task_run_name,
                    "mensagem": log.message,
                }
                for log in logs
            ]
        )
        conn.execute(stmt)
