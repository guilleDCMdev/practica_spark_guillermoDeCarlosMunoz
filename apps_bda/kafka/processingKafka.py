from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, current_timestamp

# Credenciales de acceso
AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'

# Definición de nombres de columnas
COL_TIMESTAMP = 'timestamp'
COL_STORE = 'store_code'
COL_PRODUCT = 'item_code'
COL_QUANTITY = 'units_sold'
COL_REVENUE = 'sales_amount'
COL_PROCESSED = 'Processed'
COL_INSERT_DATE = 'Insertion_Date'

# Configuración de la sesión Spark
spark = SparkSession.builder \
    .appName("DataCleaningPipeline") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Ruta de los datos
source_path = "s3a://guille-bucket/sales_compacted/*.json"
df = spark.read.json(source_path)

# Lista de valores inválidos
INVALID_VALUES = ["", "None", "STORE_ERROR", "PRODUCT_ERROR", "QUANTITY_ERROR", "REVENUE_ERROR", "TIMESTAMP_ERROR"]

# Filtrar datos inválidos
filtered_df = df.filter(~(df[COL_TIMESTAMP].isin(INVALID_VALUES) | df[COL_TIMESTAMP].isNull() | (df[COL_TIMESTAMP] == 'None')))
timestamp_counts = filtered_df.groupBy(COL_TIMESTAMP).count()
most_common_timestamp = timestamp_counts.orderBy(col('count').desc()).first()[COL_TIMESTAMP]

# Cálculo de valores medios
avg_quantity = int(df.select(mean(col(COL_QUANTITY))).collect()[0][0])
avg_revenue = df.select(mean(col(COL_REVENUE))).collect()[0][0]

# Marcado de registros procesados
df = df.withColumn(COL_PROCESSED, when(
    df[COL_REVENUE].isin(INVALID_VALUES) | df[COL_REVENUE].isNull() | 
    df[COL_QUANTITY].isin(INVALID_VALUES) | df[COL_QUANTITY].isNull() | 
    df[COL_TIMESTAMP].isin(INVALID_VALUES) | df[COL_TIMESTAMP].isNull(), True).otherwise(False))

# Imputación de valores inválidos
df = df.withColumn(COL_TIMESTAMP, when(df[COL_TIMESTAMP].isin(INVALID_VALUES) | df[COL_TIMESTAMP].isNull(), most_common_timestamp).otherwise(df[COL_TIMESTAMP]))
df = df.filter(~(df[COL_STORE].isin(INVALID_VALUES) | df[COL_STORE].isNull()))
df = df.filter(~(df[COL_PRODUCT].isin(INVALID_VALUES) | df[COL_PRODUCT].isNull()))
df = df.withColumn(COL_QUANTITY, when(df[COL_QUANTITY].isin(INVALID_VALUES) | df[COL_QUANTITY].isNull(), avg_quantity).otherwise(df[COL_QUANTITY]))
df = df.withColumn(COL_REVENUE, when(df[COL_REVENUE].isin(INVALID_VALUES) | df[COL_REVENUE].isNull(), avg_revenue).otherwise(df[COL_REVENUE]))
df = df.withColumn(COL_INSERT_DATE, current_timestamp())

# Eliminar duplicados
df = df.dropDuplicates()

# Cálculo de IQR y eliminación de valores atípicos
q1_qty, q3_qty = df.selectExpr(
    f"percentile_approx({COL_QUANTITY}, 0.25)",
    f"percentile_approx({COL_QUANTITY}, 0.75)").first()
iqr_qty = q3_qty - q1_qty
low_qty, high_qty = q1_qty - 1.5 * iqr_qty, q3_qty + 1.5 * iqr_qty

df = df.filter((col(COL_QUANTITY) >= low_qty) & (col(COL_QUANTITY) <= high_qty))

q1_rev, q3_rev = df.selectExpr(
    f"percentile_approx({COL_REVENUE}, 0.25)",
    f"percentile_approx({COL_REVENUE}, 0.75)").first()
iqr_rev = q3_rev - q1_rev
low_rev, high_rev = q1_rev - 1.5 * iqr_rev, q3_rev + 1.5 * iqr_rev

df = df.filter((col(COL_REVENUE) >= low_rev) & (col(COL_REVENUE) <= high_rev))

# Conversión de tipos de datos
df = df.withColumn(COL_TIMESTAMP, (col(COL_TIMESTAMP) / 1000).cast("timestamp")) \
       .withColumn(COL_STORE, col(COL_STORE).cast("int")) \
       .withColumn(COL_PRODUCT, col(COL_PRODUCT).cast("string")) \
       .withColumn(COL_QUANTITY, col(COL_QUANTITY).cast("int")) \
       .withColumn(COL_REVENUE, col(COL_REVENUE).cast("double"))

df.show()
df.printSchema()

# Escritura de los datos procesados
df.write \
    .format('csv') \
    .option('header', 'true') \
    .option('fs.s3a.committer.name', 'partitioned') \
    .option('fs.s3a.committer.staging.conflict-mode', 'replace') \
    .option("fs.s3a.fast.upload.buffer", "bytebuffer") \
    .mode('overwrite') \
    .csv(path='s3a://guille-bucket/kafka_processed', sep=',')
