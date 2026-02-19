import sqlalchemy as sa
from pydantic.dataclasses import dataclass
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


# Atenção, não é utilizado para Migrations
@dataclass
class PipelineParams:
    refresh_cache: bool
    ignore_tasks: list[str]
    ignore_flows: list[str]
    message: str | None


class Lote(Base):
    __tablename__ = "lote"

    id = sa.Column(sa.Integer, sa.Identity(start=1, cycle=False), primary_key=True)
    todos_flows_ok = sa.Column(sa.Boolean, nullable=False, server_default=sa.false())
    data_inicio_lote = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    )
    data_fim_lote = sa.Column(sa.TIMESTAMP(timezone=True), nullable=True)
    data_inicio_extract = sa.Column(
        sa.Date,
        nullable=False,
    )
    data_fim_extract = sa.Column(sa.Date, nullable=False)
    flows_ignoradas = sa.Column(sa.String(25), nullable=True)
    tasks_ignoradas = sa.Column(sa.String(1500), nullable=True)
    resetar_cache = sa.Column(sa.Boolean, nullable=False)
    mensagem = sa.Column(sa.Text, nullable=True)
    urls_nao_baixadas = sa.Column(sa.Boolean, nullable=False, server_default=sa.false())


class ErrosExtract(Base):
    __tablename__ = "erros_extract"

    id = sa.Column(sa.Integer, sa.Identity(start=1, cycle=False), primary_key=True)
    lote_id = sa.Column(sa.Integer, sa.ForeignKey("lote.id"), nullable=False)
    task = sa.Column(sa.String(50), nullable=False)
    data_hora = sa.Column(
        sa.DateTime(timezone=True),
        nullable=False,
        server_default=sa.func.now(),
    )
    status_code = sa.Column(sa.Integer, nullable=True)
    mensagem = sa.Column(sa.String(500), nullable=True)
    url = sa.Column(sa.Text, nullable=False, unique=True)
