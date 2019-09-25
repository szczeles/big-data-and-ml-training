# Airflow with Spark task

## Build with

    docker build -t dataops/airflow:latest .

## Copy job into container

    docker cp stats.py airflow:/usr/local/airflow/jobs

## Copy DAG into container

    docker cp dag.py airflow:/usr/local/airflow/dags

## Airflow Connection

* remove queue param
