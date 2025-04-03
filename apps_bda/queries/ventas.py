from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, max, to_timestamp, lit

aws_access_key_id = 'test'
aws_secret_access_key = 'test'

date= 'date'
stores= 'store_ID'
name= 'store_name'
location= 'location'
demographics= 'demographics'
products= 'product_ID'
quantity= 'quantity_Sold'
revenue= 'revenue'
tratado= 'Tratado'
fecha_insercion= 'fecha_Insercion'

spark = SparkSession.builder \
    .appName("csvTransformData") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

bucket_path_csv = "s3a://bucket-1/CSV_Limpio/*.csv"
df_csv = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path_csv)

bucket_path_db = "s3a://bucket-1/Postgres_Limpio/*.csv"
df_db = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path_db)

bucket_path_kafka= "s3a://bucket-1/Kafka_Limpio/*.csv"
df_kafka = spark.read.option('header', 'true').option("delimiter", ",").csv(bucket_path_kafka)

# Renombrar columnas en df_csv y df_kafka para que coincidan
df_csv = df_csv.withColumnRenamed("date", date) \
             .withColumnRenamed("store_ID", stores) \
             .withColumnRenamed("product_ID", products) \
             .withColumnRenamed("quantity_Sold", quantity) \
             .withColumnRenamed("revenue", revenue) \
             .withColumnRenamed("Tratado", tratado) \
             .withColumnRenamed("Fecha Insercion", fecha_insercion)

df_kafka = df_kafka.withColumnRenamed("timestamp", date) \
                 .withColumnRenamed("store_id", stores) \
                 .withColumnRenamed("product_id", products) \
                 .withColumnRenamed("quantity_sold", quantity) \
                 .withColumnRenamed("revenue", revenue) \
                 .withColumnRenamed("Tratado", tratado) \
                 .withColumnRenamed("Fecha Insercion", fecha_insercion)

# Seleccionar y ordenar columnas para que coincidan antes de la unión
columns_union = [date, stores, products, quantity, revenue, tratado, fecha_insercion]
df_csv = df_csv.select(columns_union)
df_kafka = df_kafka.select(columns_union)

df_union = df_csv.union(df_kafka)

df_joined = df_union.join(df_db, stores, "inner")

df_grouped = df_joined.groupBy(col(stores)).agg(sum(col(revenue)).alias("total_revenue"))
max_revenue = df_grouped.agg(max(col("total_revenue"))).collect()[0][0]
df_stores_max = df_grouped.filter(col("total_revenue") == max_revenue)
df_top_stores = df_stores_max.join(df_db, stores, "inner")

df_quantity_date= df_union.groupBy(col(date)).agg(sum(col(quantity)).alias('total_sold'))

df_quantiy_date_exact= df_quantity_date.filter(col(date) == to_timestamp(lit('1974-10-18'), 'yyyy-MM-dd')) 

df_products_group = df_union.groupBy(col(products)).agg(sum(col(quantity)).alias('total_sold'))
max_sold = df_products_group.agg(max(col("total_sold"))).collect()[0][0]
df_products_max = df_products_group.filter(col("total_sold") == max_sold)
df_top_product = df_products_max.join(df_union, products, "inner")

print('\n Tienda con mas beneficios de venta: \n')

df_top_stores.show()

print('Ventas en una fecha exacta: \n')

df_quantiy_date_exact.show()

print('Producto mas vendido: \n')

df_top_product.show()