# ------------------------------------------
# DAG de ingestão da SPTrans (Olho Vivo)
# Executa o script de coleta a cada 2 minutos
# ------------------------------------------

# Importa classes básicas do Airflow
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Criação da DAG — tudo dentro deste bloco "with" é o pipeline
with DAG(
    dag_id="ingest_sptrans_cada_2_min",        # nome único da DAG (vai aparecer na UI do Airflow)
    start_date=datetime(2025, 10, 1),          # data de início (obrigatório no Airflow)
    schedule="*/2 * * * *",                    # cron: executa a cada 2 minutos
    catchup=False,                             # não executa retroativamente
    tags=["sptrans", "ingestao", "bronze"],    # categorias para facilitar busca na UI
    description="Ingestão Olho Vivo SPTrans a cada 2 minutos (camada Bronze)",
):

    # Define a tarefa (task) que o Airflow vai executar
    ingest = BashOperator(
        task_id="ingest_posicao",              # nome interno da tarefa
        bash_command="python /opt/airflow/scripts/ingest_sptrans.py",  # comando executado
    )

    # Aqui poderíamos adicionar outras tasks futuramente (como Spark, dbt, etc.)
    # Exemplo: ingest >> spark_process >> dbt_run