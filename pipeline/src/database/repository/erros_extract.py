from prefect.logging import get_logger
from sqlalchemy import insert

from database.engine import get_connection
from database.models.base import ErrosExtract

erros_extract = ErrosExtract.__table__

logger = get_logger()


def insert_extract_error_db(
    lote_id: int, task: str, status_code: int | None, message: str | None, url: str
):
    """
    Cria um novo registro na tabela erros_extract de uma URL que não pôde ser baixada.
    Recebe como argumento o nome da task, o código de status de erro, a mensagem de erro e a URL
    """
    try:
        with get_connection() as conn:
            stmt = insert(erros_extract).values(
                lote_id=lote_id,
                task=task,
                status_code=status_code,
                mensagem=message,
                url=url,
            )
            _result = conn.execute(stmt)
    except Exception as e:
        logger.error(f"Erro ao inserir erro de extract na tabela erros_extract: {e}")
