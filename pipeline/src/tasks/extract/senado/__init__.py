from .colegiados import extract_colegiados
from .despesas_senadores import extract_despesas_senado
from .detalhes_processos import extract_detalhes_processos_senado
from .detalhes_senadores import extract_detalhes_senadores_senado
from .discursos_senadores import extract_discursos_senado
from .processos_senado import extract_processos_senado
from .senadores import extract_senadores_senado
from .votacoes import extract_votacoes_senado

__all__ = [
    "extract_colegiados",
    "extract_senadores_senado",
    "extract_detalhes_senadores_senado",
    "extract_discursos_senado",
    "extract_despesas_senado",
    "extract_processos_senado",
    "extract_detalhes_processos_senado",
    "extract_votacoes_senado",
]
