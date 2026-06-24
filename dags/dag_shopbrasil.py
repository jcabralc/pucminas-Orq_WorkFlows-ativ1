"""
Atividade 1 - Airflow - Orquestração de Workflows
"""

from airflow.decorators import dag, task
from groups.ingestion import build_ingestion_group
from groups.analysis import build_analysis_group
import pendulum
from datetime import datetime, timedelta

# Definição estrita do Timezone de São Paulo usando Pendulum
local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'owner': 'Data Team - ShopBrasil',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime(2026, 6, 20)
}

@dag(
    dag_id='shopbrasil_ecommerce_pipeline',
    default_args=default_args,
    description='Pipeline atividade 1.',
    schedule_interval='0 6 * * *', # Execução diária às 06:00 horário de Brasília
    start_date=pendulum.datetime(2026, 6, 20, tz=local_tz),
    catchup=False,
    tags=['shopbrasil', 'atividade1'],
)
def shopbrasil_pipeline():
    # 1. Topologia Linear - Ingestão de dados e validação de schema
    ingestion_group, validated_data = build_ingestion_group(group_id="grupo_ingestao")

    # 2. Topologia Fan-Out e Fan-In - Análise com processamento distribuído por categoria mapeada
    analysis_group = build_analysis_group(group_id="grupo_analise", validated_products=validated_data)

    # Definição das dependências entre os blocos (TaskGroups) baseado no fluxo de dados
    ingestion_group >> analysis_group

shopbrasil_pipeline()