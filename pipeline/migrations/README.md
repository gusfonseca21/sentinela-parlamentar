# Gerenciando Migrations

1. Definimos/alteramos os modelos das tabelas em /migrations/models
2. Geramos o código da migração de forma automática com o comando `uv run alembic revision --autogenerate -m "mensagem"`
3. Atualizamos o banco de dados com o comando `uv run alembic upgrade head` ou `uv run alembic upgrade +1`

- Podemos voltar para o início das versões com `uv run alembic downgrade base` ou `uv run alembic downgrade +1`

- Podemos gerar um script SQL para a migração, em vez de ser online: `alembic upgrade head --sql > ./migrations/migration.sql`
