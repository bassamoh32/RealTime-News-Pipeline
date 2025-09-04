import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, monotonically_increasing_id
from pyspark.sql.types import StructType, StringType
import yaml

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from cassandra_utils.cassandra_manager import CassandraManager

# Load configurations
def load_config(config_file):
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'configuration', config_file)
    with open(path) as f:
        return yaml.safe_load(f)

kafka_config = load_config('kafka.yml')
cassandra_config = load_config('cassandra.yml')

# Cassandra connection
cassandra = CassandraManager(
    host=cassandra_config['HOST'],
    keyspace=cassandra_config['KEYSPACE'],
    table=cassandra_config['TABLE'],
    username=cassandra_config['USERNAME'],
    password=cassandra_config['PASSWORD']
)
cassandra.connect()

# Process each micro-batch
def process_batch(df, batch_id):
    if df.head(1):
        print(f"[BATCH {batch_id}] Processing {df.count()} records")
        
        # Convert timestamp and add unique ID
        df = df.withColumn("publishedat", to_timestamp(col("publishedAt"))) \
               .withColumn("new_id", monotonically_increasing_id().cast(StringType()))
        
        # Deduplicate within batch by URL
        df = df.dropDuplicates(["url"])
        
        # Select final columns for Cassandra
        final_df = df.select(
            "category", "publishedat", "new_id",
            "title", "description", "url", "source"
        )
        
        # Write to Cassandra
        final_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=cassandra_config['TABLE'], keyspace=cassandra_config['KEYSPACE']) \
            .mode("append") \
            .save()
        
        print(f"[BATCH {batch_id}] Successfully written to Cassandra")
    else:
        print(f"[BATCH {batch_id}] Empty batch, skipping")

# Start Spark streaming
def start_spark():
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", ",".join([
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
            "com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"
        ])) \
        .config("spark.cassandra.connection.host", cassandra_config['HOST']) \
        .config("spark.cassandra.auth.username", cassandra_config.get('USERNAME', '')) \
        .config("spark.cassandra.auth.password", cassandra_config.get('PASSWORD', '')) \
        .getOrCreate()
    
    print("[SPARK] Spark session created")

    # Kafka schema
    schema = StructType() \
        .add("title", StringType()) \
        .add("description", StringType()) \
        .add("url", StringType()) \
        .add("publishedAt", StringType()) \
        .add("source", StringType()) \
        .add("category", StringType())

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['BROKER']) \
        .option("subscribe", kafka_config['KAFKA_TOPIC']) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
                .select(from_json(col("json_str"), schema).alias("data")) \
                .select("data.*")
    
    # WriteStream with foreachBatch ensures all messages are processed
    query = json_df.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", "/tmp/checkpoints/news") \
        .start()

    print("[STREAMING] Spark streaming started")
    query.awaitTermination()

if __name__ == "__main__":
    try:
        start_spark()
    finally:
        cassandra.close()
