from .colegiados import extract_colegiados
from .despesas_senadores import extract_despesas_senadores
from .detalhes_processos import extract_detalhes_processos
from .detalhes_senadores import extract_detalhes_senadores
from .discursos_senadores import extract_discursos_senadores
from .processos_senado import extract_processos
from .senadores import extract_senadores
from .votacoes import extract_votacoes

__all__ = [
    "extract_colegiados",
    "extract_senadores",
    "extract_detalhes_senadores",
    "extract_discursos_senadores",
    "extract_despesas_senadores",
    "extract_processos",
    "extract_detalhes_processos",
    "extract_votacoes",
]
