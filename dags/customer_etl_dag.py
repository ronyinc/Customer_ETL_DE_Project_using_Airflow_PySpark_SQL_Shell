from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'customer_etl_pipeline',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='customer_etl_dag',
    default_args=default_args,
    start_date=datetime(2025, 5, 8),
    schedule_interval='@daily',
    catchup=False
) as dag:

    env = Variable.get("airflow_env", default_var="dev")
    run_etl = BashOperator(
        task_id='run_customer_loyalty_etl',
        bash_command=f"bash /opt/spark-apps/customer_etl/shell/customer_etl_job_airflow.sh {env}"
    )

    run_etl