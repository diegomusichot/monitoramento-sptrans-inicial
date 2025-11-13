"""
DAG responsável pela ingestão dos dados da API Olho Vivo (SPTrans)
-----------------------------------------------------------------
Objetivo:
Executar a coleta de posições de ônibus a cada 2 minutos,
utilizando o script Python localizado em /opt/airflow/scripts/ingest_sptrans.py

Destaques:
- Executa automaticamente a cada 2 minutos (cron)
- Reexecuta em caso de falha (retries)
- Define tempo máximo de execução (timeout)
- Impede execuções simultâneas (max_active_runs)
"""

# Importações necessárias
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ------------------------------------------------------------
#CONFIGURAÇÕES PADRÃO DA DAG
# ------------------------------------------------------------
# Esses parâmetros valem para todas as tasks dentro da DAG.
# Eles controlam comportamento em caso de falha, tempo de espera e dependências.
default_args = {
    "depends_on_past": False,               # Cada execução é independente (não depende da anterior)
    "retries": 3,                           # Número de tentativas em caso de falha
    "retry_delay": timedelta(seconds=45),   # Tempo entre as tentativas
}

# ------------------------------------------------------------
# DEFINIÇÃO DA DAG
# ------------------------------------------------------------
# Aqui definimos as propriedades principais da DAG — nome, agendamento e políticas.
with DAG(
    dag_id="ingest_sptrans_cada_2_min",     # Nome único da DAG (como será exibido no Airflow)
    description="Ingestão da API Olho Vivo (SPTrans) a cada 2 minutos – camada Bronze",
    start_date=datetime(2025, 10, 1),       # Data inicial de referência (não retroage)
    schedule="*/2 * * * *",                 # Executa a cada 2 minutos (formato cron)
    catchup=False,                          # Não reexecuta períodos antigos ao ativar a DAG
    default_args=default_args,              # Aplica as configurações definidas acima
    max_active_runs=1,                      # Impede execuções simultâneas (evita sobreposição)
    tags=["sptrans", "bronze", "ingestao"], # Facilita busca e categorização na UI do Airflow
) as dag:

    # ------------------------------------------------------------
    # TASK: Executar o script de ingestão
    # ------------------------------------------------------------
    # Essa task chama diretamente o script Python que realiza a coleta dos dados.
    # O comando "set -euo pipefail" garante que qualquer erro no script cause falha imediata.
    ingest = BashOperator(
        task_id="ingest_posicao",  # Identificador da task (único dentro da DAG)
        bash_command=(
            "set -euo pipefail; "  # Torna o bash mais seguro (erro → interrompe execução)
            "python /opt/airflow/scripts/ingest_sptrans.py"
        ),
        # Tempo máximo permitido para execução da task
        execution_timeout=timedelta(minutes=2),

        # Configuração de nova tentativa com backoff exponencial
        retry_exponential_backoff=True,       # Aumenta o tempo entre as tentativas (progressivo)
        max_retry_delay=timedelta(minutes=3), # Tempo máximo entre as tentativas

        # SLA (Service Level Agreement): alerta visual na UI caso ultrapasse 1 minuto
        # Isso não envia e-mails — apenas exibe um aviso na interface.
        sla=timedelta(minutes=1),
    )
    
    ingest
