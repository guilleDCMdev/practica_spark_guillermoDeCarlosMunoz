from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, FloatType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("TransformarKafkaCSV") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo en S3 (LocalStack)
s3_path = "s3a://guille-bucket/SalidaKafka/part-00000-41831fa9-827a-40e0-8158-6ce7d4cf57f4-c000.csv"

# Leer el archivo CSV sin la opción "header", luego asignamos manualmente los nombres de las columnas
df = spark.read.option("delimiter", ",").csv(s3_path)

# Asignar los nombres de las columnas manualmente, el campo 'date' será 'timestamp'
df = df.toDF("timestamp", "store_id", "product_id", "quantity_sold", "revenue")

# TRANSFORMACIÓN

# Imputación para columnas numéricas con la media
numerical_columns = ['store_id', 'quantity_sold', 'revenue']

for column in numerical_columns:
    mean_value = df.agg({column: 'mean'}).collect()[0][0]
    df = df.na.fill({column: mean_value})

# Supresión de filas con valores nulos en la columna 'timestamp' (si es crítica)
df = df.dropna(subset=['timestamp'])

# Eliminar duplicados basados en ciertas columnas
df = df.dropDuplicates(['store_id', 'product_id'])

# Conversión de columnas a tipos numéricos
df = df.withColumn("store_id", df["store_id"].cast(IntegerType()))
df = df.withColumn("quantity_sold", df["quantity_sold"].cast(IntegerType()))
df = df.withColumn("revenue", df["revenue"].cast(FloatType()))

# Conversión de la columna 'timestamp' a tipo timestamp
df = df.withColumn("timestamp", df["timestamp"].cast("timestamp"))

# Agregar columna 'Tratados' con valor 'Sí' o 'No'
df = df.withColumn("Tratados", when(df["quantity_sold"].isNotNull(), "Sí").otherwise("No"))

# Agregar columna 'Fecha Inserción' con la fecha y hora actual (UTC)
df = df.withColumn("Fecha Inserción", current_timestamp())

# Mostrar los primeros 10 registros para verificar
df.show(10)

# Calcular la media y la desviación estándar de la columna 'revenue'
revenue_mean = df.agg({"revenue": "mean"}).collect()[0][0]
revenue_stddev = df.agg({"revenue": "stddev"}).collect()[0][0]

# Definir un rango de valores válidos (media +/- 3 veces la desviación estándar)
df = df.filter((df["revenue"] >= revenue_mean - 3 * revenue_stddev) & (df["revenue"] <= revenue_mean + 3 * revenue_stddev))

# Guardar el DataFrame procesado de vuelta en S3
output_path = "s3a://guille-bucket/SalidaKafka_Clean/"
df.write.option("header", "true").csv(output_path)

# Detener la sesión de Spark
spark.stop()
