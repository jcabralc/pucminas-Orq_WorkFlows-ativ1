from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from operators.validate_products_operator import ValidarProdutosOperator
import requests
import logging

# Callbacks de ciclo de vida solicitados para a Task crítica
def on_failure_callback(context):
    logging.error(f"A Task crítica '{context['task_instance'].task_id}' falhou :(.")

def on_retry_callback(context):
    logging.warning(f"Retry automático disparado para a Task '{context['task_instance'].task_id}'.")

def on_success_callback(context):
    logging.info(f"Sucesso na execução da Task '{context['task_instance'].task_id}'.")


def build_ingestion_group(group_id: str) -> TaskGroup:
    with TaskGroup(group_id=group_id) as tg:

        @task( # Buscar Proutos
            retries=3,
            retry_delay=5,  # segundos
            retry_exponential_backoff=True,
            on_failure_callback=on_failure_callback,
            on_retry_callback=on_retry_callback,
            on_success_callback=on_success_callback
        )
        def fetch_products_api():
            url = "https://fakestoreapi.com/products"
            try:
                response = requests.get(url, timeout=15)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logging.error(f"Erro ao consultar a FakeStore API: {str(e)}")
                raise e

        products_data = fetch_products_api()
        
        # Uso do Operador Customizado integrado ao fluxo do TaskGroup
        validate_schema = ValidarProdutosOperator(
            task_id="validar_schema_produtos",
            products_input=products_data
        )
        
    return tg, validate_schema.output