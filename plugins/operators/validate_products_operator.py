from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
import logging

class ValidarProdutosOperator(BaseOperator):
    """
    Operador customizado para validar o schema dos produtos capturados da API.
    Garante que os campos mandatórios existem e estão povoados.
    """

    template_fields = ("products_input",)

    def __init__(self, products_input, **kwargs):
        super().__init__(**kwargs)
        self.products_input = products_input

    def execute(self, context):
        products = self.products_input
        
        if not products or not isinstance(products, list):
            raise AirflowException(
                f"Validação falhou: O payload de produtos está vazio ou não é uma lista. "
                f"Tipo recebido: {type(products)}"
            )

        required_fields = ['id', 'title', 'price', 'category']
        
        for index, product in enumerate(products):
            for field in required_fields:
                if field not in product or product[field] is None:
                    raise AirflowException(
                        f"Erro de Schema no produto do índice {index}: Campo mandatório '{field}' ausente ou nulo."
                    )
        
        logging.info(f"Sucesso: {len(products)} produtos validados com base no schema definido.")
        return products