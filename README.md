# Prisma do Congresso

Pipeline para a coleta e armazenamento de dados relacionados à atividade parlamentar do Congresso Nacional.

## Pré-requisitos

- **[UV](https://docs.astral.sh/uv/)** - Gerenciador de pacotes moderno para Python

## Instalação

Clone o repositório e instale as dependências do projeto:

```bash
git clone https://github.com/gusfonseca21/prisma-do-congresso
cd prisma-do-congresso/pipeline
uv sync
```

## Configuração e Execução

### 1. Iniciar o Servidor Prefect

Abra um terminal no diretório raiz do projeto e execute:

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
