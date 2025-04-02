from pyspark.sql import SparkSession
from pyspark.sql.functions import  asc

aws_access_key_id='test'
aws_secret_access_key='test'

def read_from_postgres():
    spark = SparkSession.builder \
        .appName("ReadFromPostgres") \
        .config("spark.jars", "/opt/spark-apps/postgresql-42.7.3.jar") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages","org.apache.spark:spark-hadoop-cloud_2.13:3.5.1,software.amazon.awssdk:s3:2.25.11") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.driver.extraClassPath", "/opt/spark-apps/postgresql-42.7.3.jar,/opt/spark/jars/s3-2.25.11.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/s3-2.25.11.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()

    # Define connection properties
    jdbc_url = "jdbc:postgresql://postgres-db:5432/retail_db"
    connection_properties = {
        "user": "postgres",
        "password": "casa1234",
        "driver": "org.postgresql.Driver"
    }

    table_name = "stores"
    try:
        # Read data from PostgreSQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
        
        df.sort(asc("store_name")) \
        .write \
        .option('fs.s3a.committer.name', 'partitioned') \
        .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
        .option("fs.s3a.fast.upload.buffer", "bytebuffer")\
        .mode('overwrite') \
        .csv(path='s3a://guille-bucket/SalidaBD', sep=',')


    except Exception as e:
        print("Error reading data from PostgreSQL:", e)
    finally:
        spark.stop()

if __name__ == "__main__":
    read_from_postgres()