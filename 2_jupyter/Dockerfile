FROM jupyter/pyspark-notebook

USER root

COPY spark-defaults.conf /usr/local/spark/conf/

RUN pip install --no-cache-dir git+https://github.com/szczeles/avro_gen.git@py3_compatibility

RUN mkdir /etc/hadoop && \
    echo '<configuration><property><name>hive.metastore.uris</name><value>thrift://hive-metastore:9083</value></property></configuration>' > /etc/hadoop/hive-site.xml && \
    echo "<configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:9000</value></property></configuration>" > /etc/hadoop/core-site.xml && \
    echo "<configuration><property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property></configuration>" > /etc/hadoop/yarn-site.xml

ENV HADOOP_CONF_DIR=/etc/hadoop

USER jovyan
