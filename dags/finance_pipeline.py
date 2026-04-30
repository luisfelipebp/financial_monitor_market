from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/dags")

from crypto import run_pipeline_crypto
from stocks import run_pipeline_stocks

default_args= {
    'owner': 'luisfelipebp',
    'retry':3,
    'retry_delay': timedelta(5)
}

with DAG(
    dag_id="dag_pipeline",
    default_args=default_args,
    description="Orquestração paralela de Ações e Criptomoedas",
    start_date=datetime(2026,1,1),
    schedule_interval="@hourly",
    catchup=False,    
)as dag:
    
    task_run_pipeline_crypto = PythonOperator(
        task_id="executar_pipeline_crypto",
        python_callable=run_pipeline_crypto
    )
    task_run_pipeline_stocks = PythonOperator(
        task_id="executar_pipeline_stocks",
        python_callable=run_pipeline_stocks
    )

    run_dbt_transformations = BashOperator(
        task_id="run_dbt_transformations",
        bash_command= "cd /opt/airflow/dbt_finance && dbt run --profiles-dir . && dbt test --profiles-dir ."
    )

    [task_run_pipeline_crypto, task_run_pipeline_stocks] >> run_dbt_transformations



