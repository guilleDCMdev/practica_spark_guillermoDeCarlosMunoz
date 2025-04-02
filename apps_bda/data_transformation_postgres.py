from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp
from pyspark.sql.types import IntegerType, StringType

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("TransformarPostgresCSV") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Ruta del archivo en S3 (LocalStack)
s3_path = "s3a://guille-bucket/Postgres/postgres_data.csv"

# Leer el archivo CSV con la opción "header=True" para que Spark detecte la cabecera
df = spark.read.option("delimiter", ",").csv(s3_path, header=True)

# Mostrar los primeros registros para verificar que Spark detectó correctamente las cabeceras
df.show(10)

# TRANSFORMACIÓN

# Imputación para columnas numéricas con la media (aquí asumimos que algunas columnas numéricas, ajusta según tu esquema)
numerical_columns = ['store_id']  # En este caso solo 'store_id' parece numérica, ajustar según tu archivo

for column in numerical_columns:
    mean_value = df.agg({column: 'mean'}).collect()[0][0]
    df = df.na.fill({column: mean_value})

# Supresión de filas con valores nulos en la columna 'store_name' (si es crítica)
df = df.dropna(subset=['store_name'])

# Eliminar duplicados basados en ciertas columnas (por ejemplo, 'store_id' y 'store_name')
df = df.dropDuplicates(['store_id', 'store_name'])

# Conversión de columnas a tipos adecuados (store_id como entero, las demás como String)
df = df.withColumn("store_id", df["store_id"].cast(IntegerType()))
df = df.withColumn("store_name", df["store_name"].cast(StringType()))
df = df.withColumn("location", df["location"].cast(StringType()))
df = df.withColumn("demographics", df["demographics"].cast(StringType()))

# Agregar columna 'Tratados' con valor 'Sí' o 'No' (si 'store_name' es nulo)
df = df.withColumn("Tratados", when(df["store_name"].isNotNull(), "Sí").otherwise("No"))

# Agregar columna 'Fecha Inserción' con la fecha y hora actual (UTC)
df = df.withColumn("Fecha Inserción", current_timestamp())

# Mostrar los primeros 10 registros para verificar
df.show(10)

# Guardar el DataFrame procesado de vuelta en S3
output_path = "s3a://guille-bucket/Postgres_Clean/"
df.write.option("header", "true").csv(output_path)

# Detener la sesión de Spark
spark.stop()
