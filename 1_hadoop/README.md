# Build images

```
docker build -f Dockerfile-metastore -t dataops/metastore .
docker build -f Dockerfile-nodemanager -t dataops/nodemanager .
```

# YARN

## Running example

```
yarn jar /opt/hadoop-2.7.4/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.7.4.jar pi 16 1000
```

# Hive

## Connecting via JDBC

```
beeline -u 'jdbc:hive2://hive-server:10000'
```

## Sample data load 

```
CREATE TABLE pokes (foo INT, bar STRING);
LOAD DATA LOCAL INPATH '/opt/hive/examples/files/kv1.txt' OVERWRITE INTO TABLE pokes;
```

## Client container

```
docker run --rm --env-file hadoop.env --network minicluster --entrypoint bash -ti bde2020/hadoop-datanode:2.0.0-hadoop2.7.4-java8
```
