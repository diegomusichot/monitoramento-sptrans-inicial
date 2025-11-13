from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# DAG simples: Silver -> Refinado -> Gold -> Último JSON na Gold
# A DAG de ingestão continua separada (rodando a cada 2 min).

default_args = {
    "owner": "diego",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="pipeline_sptrans",
    description="Silver -> Refinado -> Gold (simples, sem DQ) + export do último JSON",
    start_date=datetime(2025, 11, 1),
    schedule_interval="*/3 * * * *",   # a cada 3 minutos
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=10),
    tags=["sptrans","silver","gold"],
) as dag:

    transformar_silver = BashOperator(
        task_id="transformar_silver",
        bash_command="python /opt/airflow/scripts/transform_sptrans.py",
    )

    refinar_silver = BashOperator(
        task_id="refinar_silver",
        bash_command="python /opt/airflow/scripts/refinar_sptrans.py",
    )

    build_gold = BashOperator(
        task_id="build_gold_kpis",
        bash_command="python /opt/airflow/scripts/build_gold_kpis.py",
    )

    export_ultimo_json = BashOperator(
        task_id="export_ultimo_json_gold",
        bash_command="python /opt/airflow/scripts/export_ultimo_json_gold.py",
    )

    # Ordem de execução da DAG:
    transformar_silver >> refinar_silver >> build_gold >> export_ultimo_json
