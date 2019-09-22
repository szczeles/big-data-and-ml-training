# Jupyter with PySpark

## Building the image

    docker build -t dataops/jupyter:latest .

## Sample data location

* text: `file:///usr/local/spark/examples/src/main/resources/people.txt`
* CSV: `file:///usr/local/spark/examples/src/main/resources/people.csv`
* JSON: `file:///usr/local/spark/examples/src/main/resources/people.json`
* AVRO: `file:///usr/local/spark/examples/src/main/resources/users.avro`
* ORC: `file:///usr/local/spark/examples/src/main/resources/users.orc`
* PARQUET: `file:///usr/local/spark/examples/src/main/resources/users.parquet`

## Reading from MySQL

[Official documentation on JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)

#### MySQL Server Setup

    docker run --name some-mysql --network minicluster -e MYSQL_ROOT_PASSWORD=my-secret-pw -d mysql:latest
    docker exec -ti some-mysql mysql -pmy-secret-pw

#### Sample data load

[List of countries](https://raw.githubusercontent.com/raramuridesign/mysql-country-list/master/mysql-country-list.sql)

#### Using data in Spark

```
reader = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://some-mysql")  \
    .option("dbtable", '<TODO>') \
    .option("user", 'root') \
    .option("password", 'my-secret-pw') \
    .option('driver', 'com.mysql.cj.jdbc.Driver') \
    .load()
```
