from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

DDL = """
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS staging.sptrans_posicao_raw (
  id SERIAL PRIMARY KEY,
  dt TEXT NOT NULL,
  hh TEXT NOT NULL,
  snapshot_ts TEXT NOT NULL,
  raw JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS serving.sptrans_posicao (
  dt DATE NOT NULL,
  hh TEXT NOT NULL,
  snapshot_ts TIMESTAMP WITHOUT TIME ZONE,
  prefixo TEXT,
  acessivel BOOLEAN,
  hora_posicao TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  sentido_viagem TEXT,
  id_veiculo TEXT,
  CONSTRAINT pk_serving PRIMARY KEY (dt, hh, prefixo, snapshot_ts)
);
"""

with DAG(
    dag_id="init_postgres_sptrans",
    start_date=datetime(2025, 10, 1),
    schedule=None,           # roda sob demanda
    catchup=False,
    tags=["setup","postgres"],
) as dag:

    create_struct = PostgresOperator(
        task_id="create_db_struct",
        postgres_conn_id="postgres_sptrans",  # <- usa a Connection da UI
        sql=DDL,
    )
 
