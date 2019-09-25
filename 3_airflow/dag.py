from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1)
}

dag = DAG('stackoverflow_stats', default_args=default_args,
          schedule_interval='@daily')

SparkSubmitOperator(
    task_id='get-stats',
    application="/usr/local/airflow/jobs/stats.py",
    dag=dag,
    run_as_user='airflow',
    application_args=['--date', '{{ ds }}'],
    name='Stats DAG for {{ ds }}',
    num_executors=2,
    executor_memory='2g'
)
