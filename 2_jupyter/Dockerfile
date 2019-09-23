FROM jupyter/pyspark-notebook

USER root

COPY spark-defaults.conf /usr/local/spark/conf/

RUN pip install --no-cache-dir git+https://github.com/szczeles/avro_gen.git@py3_compatibility

USER jovyan
