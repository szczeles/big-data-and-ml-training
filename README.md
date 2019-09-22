# Training: Big Data and Machine Learning with Spark

## Prerequisites

1. Check that your MS Windows system has at least 40GB of free space and 4GB of free RAM. Before cloning the repo content disable line endings conversion:

        git config --global core.autocrlf false

2. Re-create default docker machine using:

        docker-machine rm default
        docker-machine create -d virtualbox --virtualbox-memory=4096 \
            --virtualbox-cpu-count=4 --virtualbox-disk-size=40960 \
            --virtualbox-no-vtx-check default
            
3. Pull images that we are going to need during the training:

        docker pull bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8
        docker pull bde2020/hive:2.3.2-postgresql-metastore
        docker pull jupyter/pyspark-notebook
        docker pull puckel/docker-airflow

4. Create docker network that we will use to across cluster nodes:

        docker network create minicluster
