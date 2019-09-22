FROM jupyter/pyspark-notebook

USER root

COPY spark-defaults.conf /usr/local/spark/conf/


#    echo spark.hadoop.fs.defaultFS=hdfs://namenode:9000 | tee -a /opt/conda/lib/python3.7/site-packages/pyspark/conf/spark-defaults.conf && \
#    echo spark.jars.packages=org.apache.spark:spark-avro_2.11:2.4.4,mysql:mysql-connector-java:8.0.17 | tee -a /opt/conda/lib/python3.7/site-packages/pyspark/conf/spark-defaults.conf

#RUN conda install --quiet --yes \
#    matplotlib && \
#    conda clean --all -f -y && \
#    pip install --no-cache-dir avro-gen

RUN pip install --no-cache-dir git+https://github.com/szczeles/avro_gen.git@py3_compatibility

USER jovyan
