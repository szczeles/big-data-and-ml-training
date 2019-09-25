FROM puckel/docker-airflow

USER root

RUN mkdir -p /usr/share/man/man1 &&  \
    apt update && apt install -y openjdk-8-jre-headless && \
    pip install pyspark && \
    mkdir /usr/local/airflow/hadoop-config && \
    echo '<configuration><property><name>hive.metastore.uris</name><value>thrift://hive-metastore:9083</value></property></configuration>' > hadoop-config/hive-site.xml && \
    echo "<configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property></configuration>" > hadoop-config/core-site.xml && \
    echo "<configuration><property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property></configuration>" > hadoop-config/yarn-site.xml

RUN mkdir /usr/local/airflow/jobs && mkdir /usr/local/airflow/dags && chown -R airflow /usr/local/airflow

ENV HADOOP_CONF_DIR=/usr/local/airflow/hadoop-config

USER airflow
