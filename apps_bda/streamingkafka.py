from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id} with {batch_df.count()} records.")
    if batch_df.count() > 0:
        print(f"Batch {batch_id} has data.")
        batch_df.show()  # Log the batch data
        batch_df.persist()
        try:
            # Write the DataFrame to S3
           batch_df \
            .write \
            .option('fs.s3a.committer.name', 'partitioned') \
            .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
            .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
            .option("header", "true") \
            .mode('append') \
            .json(path='s3a://guille-bucket/sales')
            
        except Exception as e:
            print(f"Error writing batch {batch_id} to S3: {str(e)}")
        finally:
            batch_df.unpersist()
            print(f"Batch {batch_id} processed and unpersisted.")
    else:
        print(f"Batch {batch_id} is empty.")


try:
    spark = SparkSession.builder \
        .appName("Streaming from Kafka") \
        .config("spark.streaming.stopGracefullyOnShutdown", True) \
        .config("spark.sql.shuffle.partitions", 4) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.jars.packages", "org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    spark.conf.set("spark.hadoop.fs.s3a.connection.maximum", "100")
    spark.conf.set("spark.hadoop.fs.s3a.fast.upload", "true")
    spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark.conf.set("spark.hadoop.fs.s3a.committer.name", "partitioned")

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9093") \
        .option("subscribe", "sales_stream") \
        .load()

    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("store_id", IntegerType()) \
        .add("product_id", StringType()) \
        .add("quantity_sold", IntegerType()) \
        .add("revenue", DoubleType())

    df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.*")

    df.printSchema()

    query = df.writeStream \
        .format("json") \
        .option("path", "s3a://guille-bucket/sales") \
        .option("checkpointLocation", "s3a://guille-bucket/checkpoint")\
        .trigger(processingTime="5 seconds")\
        .foreachBatch(process_batch) \
        .start()

    query.awaitTermination()

except Exception as e:
    print("Error reading data from Kafka:", e)    
finally:
    spark.stop()

