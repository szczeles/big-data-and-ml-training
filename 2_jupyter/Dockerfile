FROM jupyter/base-notebook

USER root

RUN apt update && apt install -y openjdk-8-jre-headless && rm -rf /var/lib/apt/lists/*

RUN conda install --quiet --yes \
    'pyspark=2.4.4' && \
    conda clean --all -f -y

RUN mkdir -p /opt/conda/lib/python3.7/site-packages/pyspark/conf && \
    echo spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 | tee -a /opt/conda/lib/python3.7/site-packages/pyspark/conf/spark-defaults.conf && \
    echo spark.hadoop.fs.defaultFS=hdfs://namenode:9000 | tee -a /opt/conda/lib/python3.7/site-packages/pyspark/conf/spark-defaults.conf

USER jovyan
