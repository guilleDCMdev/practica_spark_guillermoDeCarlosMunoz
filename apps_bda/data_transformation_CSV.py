from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, FloatType
import pandas as pd
# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("VerDatosS3") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo en S3 (LocalStack)
s3_path = "s3a://guille-bucket/SalidaCSV/part-00000-bf6ae1cb-cc16-4194-8df2-2368063684dd-c000.csv"

# Leer el archivo CSV sin la opción "header"
df = spark.read.option("delimiter", ",").csv(s3_path)

# Asignar los nombres de las columnas manualmente
df = df.toDF("date", "store_id", "product_id", "quantity_sold", "revenue")

"""# Mostrar los primeros 10 registros
df.show(10)

# Mostrar el esquema para verificar que las columnas se asignaron correctamente
df.printSchema()"""


# TRANSFORMACIÓN

# Imputación para columnas numéricas con la media
numerical_columns = ['store_id', 'quantity_sold', 'revenue']  # Cambia según tus columnas numéricas

for column in numerical_columns:
    mean_value = df.agg({column: 'mean'}).collect()[0][0]
    df = df.na.fill({column: mean_value})

# Supresión de filas con valores nulos en la columna 'date' (si es crítica)
df = df.dropna(subset=['date'])

# Eliminar duplicados basados en ciertas columnas (por ejemplo, 'store_id' y 'product_id')
df = df.dropDuplicates(['store_id', 'product_id'])

# Conversión de columnas a tipos numéricos
df = df.withColumn("store_id", df["store_id"].cast(IntegerType()))
df = df.withColumn("quantity_sold", df["quantity_sold"].cast(IntegerType()))
df = df.withColumn("revenue", df["revenue"].cast(FloatType()))

# Conversión de la columna 'date' a tipo fecha
df = df.withColumn("date", df["date"].cast("date"))

# Agregar columna 'Tratados' con valor 'Sí' o 'No'
df = df.withColumn("Tratados", when(df["quantity_sold"].isNotNull(), "Sí").otherwise("No"))

# Agregar columna 'Fecha Inserción' con la fecha y hora actual (UTC)
df = df.withColumn("Fecha Inserción", current_timestamp())

df.show(10)

# Calcular la media y la desviación estándar de la columna 'revenue'
revenue_mean = df.agg({"revenue": "mean"}).collect()[0][0]
revenue_stddev = df.agg({"revenue": "stddev"}).collect()[0][0]

# Definir un rango de valores válidos (media +/- 3 veces la desviación estándar)
df = df.filter((df["revenue"] >= revenue_mean - 3 * revenue_stddev) & (df["revenue"] <= revenue_mean + 3 * revenue_stddev))

# Guardar el DataFrame procesado de vuelta en S3
output_path = "s3a://guille-bucket/CleanCSV_data/"
df.write.option("header", "true").csv(output_path)

# Detener la sesión de Spark
spark.stop()
