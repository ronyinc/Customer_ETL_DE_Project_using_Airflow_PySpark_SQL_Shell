from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# adding comments to test the feature branch flow on git - 2025-08-06

default_args = {
    'owner': 'customer_etl_pipeline',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id ='customer_etl_dag_version2', 
    default_args=default_args,
    start_date=datetime(2025, 7, 8),
    schedule=None,
    catchup=False
) as dag:
    
    run_etl = BashOperator(
        task_id='run_customer_loyalty_etl',
        bash_command='bash /opt/spark-apps/customer_etl/shell/customer_etl_job_airflow_version_3.sh {{ ds }}'
    )
    run_etl