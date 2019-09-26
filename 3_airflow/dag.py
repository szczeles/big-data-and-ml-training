from airflow.models import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 1, 1)
}

dag = DAG('predicting_closed_questions', default_args=default_args,
          schedule_interval='@daily')

retraining = SparkSubmitOperator(
    task_id='retraining',
    application="/usr/local/airflow/jobs/retraining.py",
    dag=dag,
    run_as_user='airflow',
    application_args=['--date', '{{ ds }}'],
    name='Retraining for {{ ds }}',
    num_executors=1,
    executor_memory='1g'
)

prediction= SparkSubmitOperator(
    task_id='prediction',
    application="/usr/local/airflow/jobs/prediction.py",
    dag=dag,
    run_as_user='airflow',
    application_args=['--date', '{{ ds }}'],
    name='Prediction for {{ ds }}',
    num_executors=1,
    executor_memory='1g'
)

retraining  >> prediction