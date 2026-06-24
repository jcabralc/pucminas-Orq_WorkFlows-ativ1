from airflow.utils.task_group import TaskGroup
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

def build_analysis_group(group_id: str, validated_products) -> TaskGroup:
    with TaskGroup(group_id=group_id) as tg:

        @task
        def extract_unique_categories(products):
            # Coleta dinâmica de categorias para permitir o auto-scaling do pipeline
            categories = list(set(prod['category'] for prod in products))
            logging.info(f"Categorias únicas detectadas para processamento mapeado: {categories}")
            return categories

        # Uso mandatório do Dynamic Task Mapping (.expand) com isolamento no pool de 2 slots
        @task(pool='ecommerce_pool')
        def calculate_category_metrics(category, products):
            category_products = [p for p in products if p['category'] == category]
            prices = [float(p['price']) for p in category_products]
            
            metrics = {
                'category': category,
                'quantity': len(prices),
                'avg_price': round(sum(prices) / len(prices), 2) if prices else 0.0,
                'min_price': min(prices) if prices else 0.0,
                'max_price': max(prices) if prices else 0.0
            }
            return metrics

        # Fan-in: Consolidação dos outputs mapeados e persistência idempotente no DB
        @task
        def save_metrics_to_postgres(metrics_list, **context):
            # Coleta da data de execução (logical_date/ds) do contexto do Airflow
            execution_date = context['ds']
            pg_hook = PostgresHook(postgres_conn_id='postgres_shopbrasil')
            
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            try:
                for metrics in metrics_list:
                    # 1. Escrita Idempotente no Snapshot via UPSERT (ON CONFLICT)
                    cursor.execute("""
                        INSERT INTO category_metrics_snapshot (category, quantity, avg_price, min_price, max_price, updated_at)
                        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (category) 
                        DO UPDATE SET 
                            quantity = EXCLUDED.quantity,
                            avg_price = EXCLUDED.avg_price,
                            min_price = EXCLUDED.min_price,
                            max_price = EXCLUDED.max_price,
                            updated_at = CURRENT_TIMESTAMP;
                    """, (metrics['category'], metrics['quantity'], metrics['avg_price'], metrics['min_price'], metrics['max_price']))
                    
                    # 2. Escrita Idempotente na Tabela de Histórico (Evita duplicidade em reprocessamentos na mesma data)
                    cursor.execute("""
                        INSERT INTO category_metrics_history (category, quantity, avg_price, min_price, max_price, execution_date)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (category, execution_date)
                        DO UPDATE SET
                            quantity = EXCLUDED.quantity,
                            avg_price = EXCLUDED.avg_price,
                            min_price = EXCLUDED.min_price,
                            max_price = EXCLUDED.max_price;
                    """, (metrics['category'], metrics['quantity'], metrics['avg_price'], metrics['min_price'], metrics['max_price'], execution_date))
                
                conn.commit()
                logging.info(f"Métricas gravadas com sucesso para a data {execution_date}.")
            except Exception as e:
                conn.rollback()
                logging.error(f"Erro na transação de banco de dados. Rollback executado: {str(e)}")
                raise e
            finally:
                cursor.close()
                conn.close()

        # Orquestração interna do TaskGroup de Análise
        categories = extract_unique_categories(validated_products)
        mapped_metrics = calculate_category_metrics.partial(products=validated_products).expand(category=categories)
        save_metrics_to_postgres(mapped_metrics)

    return tg