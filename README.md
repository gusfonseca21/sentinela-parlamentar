# Prisma do Congresso

Pipeline para a coleta e armazenamento de dados relacionados à atividade parlamentar do Congresso Nacional.

## Pré-requisitos

- **[UV](https://docs.astral.sh/uv/)** - Gerenciador de pacotes moderno para Python
- **[PostgreSQL](https://www.postgresql.org/)** - Sistema de Gerenciamento de Banco de Dados

## Instalação

Clone o repositório e instale as dependências do projeto:

```bash
git clone https://github.com/gusfonseca21/prisma-do-congresso
cd prisma-do-congresso/pipeline
uv sync
```

## Configuração e Execução
OBS: **Todos os comandos aqui listados devem ser executados dentro do diretório raiz do projeto pipeline**: `prisma-parlamentar/pipeline/`

### 1. Configurar Arquivo de Variáveis de Ambiente
No diretório raiz crie um arquivo .env que deve possuir as seguintes variáveis:
- **DATABASE_URL**

### 2. Sincronizar os Esquemas do Banco de Dados
No .env adicione a URL de conexão com o banco de dados que deseja ser utilizado e em seguida execute o seguinte comando:
```bash
uv run alembic upgrade head
```

### 1. Iniciar o Servidor Prefect

Execute o seguinte comando em um novo terminal:

```bash
uv run prefect server start
```

O servidor Prefect estará disponível em `http://127.0.0.1:4200`.

### 2. Criar o Deployment

Em um novo terminal, execute:

```bash
uv run src/main.py
```

### 3. Executar a Pipeline

1. Acesse a interface web do Prefect em `http://127.0.0.1:4200`
2. No menu lateral esquerdo, selecione **Deployments**
3. Clique no deployment **prisma-do-congresso-pipeline**
4. (Opcional) Para configurar quais flows executar:
   - Clique no botão com três pontos verticais (⋮) no canto superior direito
   - Selecione **Edit**
   - Em `ignore_flows`, remova `tse` e `camara` para executar todas as flows disponíveis
   - Clique em **Save** no canto inferior direito
5. Clique em **Run** → **Quick run** para iniciar a execução
6. Clique na run recém-criada para acompanhar o progresso em tempo real
