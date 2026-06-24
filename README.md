# ShopBrasil eCommerce Pipeline

# Projeto/Atividade 1 - Disciplina Orquestração de WorkFlows - Airflow

Este projeto consiste em um pipeline de dados (ETL) totalmente dockerizado, orquestrado pelo **Apache Airflow**. O objetivo é consumir dados de produtos da FakeStore API, validar o schema das informações, calcular métricas financeiras agregadas por categoria e persistir os resultados em um banco de dados PostgreSQL.

## Como Executar o Projeto

### Pre-requisitos
- Docker instalado.
- Docker Compose instalado.
- Um cliente SQL instalado localmente (ex: pgAdmin Desktop ou DBeaver)

### 1. Inicializar e Subir o Ambiente
No terminal, executar:
```bash
docker compose up -d
```

Este comando iniciará os seguintes containers de forma ordenada:

- airflow-meta-db (Postgres de Metadados do Airflow)
- airflow-atividade1-db (Postgres Destino do Data Warehouse)
- airflow-init (Inicializador automático do ecossistema)
- airflow-scheduler & airflow-webserver (Interface do Airflow)

para stopar e remover os containers:
```bash
docker compose down -v
```


### 2. Acessar as Interfaces Gráficas

#### Apache Airflow (Orquestrador)
- URL: http://localhost:8080
- Usuário: admin
- Senha: admin

Ação: Ative a DAG shopbrasil_ecommerce_pipeline para iniciar a execução do pipeline.

#### Como Conectar ao Banco de Dados (Via Cliente SQL Local)
No pgAdmin Desktop ou DBeaver, crie uma nova conexão com as seguintes credenciais:
- Host name/address: localhost ou 127.0.0.1
- Port: 5433
- Maintenance database: shopbrasil_db
- Username: atividade1
- Password: atividade123

As tabelas analíticas ´category_metrics_snapshot´ e ´category_metrics_history´ estarão disponíveis para consulta dentro do schema public do banco ´shopbrasil_db´.
