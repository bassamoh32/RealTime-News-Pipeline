#!/bin/bash
# Install dependencies
cd utils
pip install -r requirements.txt

# Start Spark consumer
spark-submit --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  --conf spark.pyspark.python=/opt/bitnami/python/bin/python3 \
  --conf spark.pyspark.driver.python=/opt/bitnami/python/bin/python3 \
  --conf spark.executorEnv.PYSPARK_PYTHON=/opt/bitnami/python/bin/python3 \
  --conf spark.driverEnv.PYSPARK_DRIVER_PYTHON=/opt/bitnami/python/bin/python3 \
  spark_consumer.py