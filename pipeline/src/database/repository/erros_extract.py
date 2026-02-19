from sqlalchemy import delete, update
from sqlalchemy.dialects.postgresql import insert

from database.engine import get_connection
from database.models.base import ErrosExtract, Lote

erros_extract = ErrosExtract.__table__
lote = Lote.__table__


def insert_extract_error_db(
    lote_id: int, task: str, status_code: int | None, message: str | None, url: str
):
    """
    Cria um novo registro na tabela erros_extract de uma URL que não pôde ser baixada.
    Recebe como argumento o nome da task, o código de status de erro, a mensagem de erro e a URL
    """
    with get_connection() as conn:
        stmt_error = (
            insert(erros_extract)
            .values(
                lote_id=lote_id,
                task=task,
                status_code=status_code,
                mensagem=message,
                url=url,
            )
            .on_conflict_do_nothing(index_elements=["url"])
        )
        _result = conn.execute(stmt_error)

        # Atualizar tabela de Lote
        stmt_lote = (
            update(lote)
            .where(lote.c.id == lote_id)
            .where(lote.c.urls_nao_baixadas.is_(False))
            .values(urls_nao_baixadas=True)
        )
        conn.execute(stmt_lote)


def verify_not_downloaded_urls_in_task_db(task: str):
    """
    Verifica se existem URLs que falharam ao serem baixadas em lotes anteriores por task.
    Retorna uma lista de URLs para serem baixadas novamente.
    Exclui da tabela os registros retornados.
    """
    with get_connection() as conn:
        stmt = (
            delete(erros_extract)
            .where(erros_extract.c.task == task)
            .returning(erros_extract.c.url)
        )
        result = conn.execute(stmt)

    return [row.url for row in result]
