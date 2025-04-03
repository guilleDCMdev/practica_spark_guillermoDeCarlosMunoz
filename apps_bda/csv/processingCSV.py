from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, lit, current_timestamp

AWS_ACCESS_KEY = 'test'
AWS_SECRET_KEY = 'test'

DATE_COL = 'Date'
STORE_COL = 'StoreID'
PRODUCT_COL = 'ProductID'
QUANTITY_COL = 'QuantitySold'
REVENUE_COL = 'Revenue'
PROCESSED_FLAG = 'Processed'
INSERT_DATE_COL = 'Insert_Date'

spark = SparkSession.builder \
    .appName("CSVDataProcessing") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

source_path = "s3a://guille-bucket/output/*.csv"
df = spark.read.option('header', 'true').option("delimiter", ",").csv(source_path)

INVALID_VALUES = ["", "STORE_ERROR", "PRODUCT_ERROR", "QUANTITY_ERROR", "REVENUE_ERROR", "DATE_ERROR"]

filtered_df = df.filter(~(df[DATE_COL].isin(INVALID_VALUES) | df[DATE_COL].isNull()))
date_counts = filtered_df.groupBy(DATE_COL).count()
most_common_date = date_counts.orderBy(col('count').desc()).first()[DATE_COL]

avg_quantity = int(df.select(mean(col(QUANTITY_COL))).collect()[0][0])
avg_revenue = df.select(mean(col(REVENUE_COL))).collect()[0][0]

df = df.withColumn(PROCESSED_FLAG, when(
    df[REVENUE_COL].isin(INVALID_VALUES) | df[REVENUE_COL].isNull() |
    df[QUANTITY_COL].isin(INVALID_VALUES) | df[QUANTITY_COL].isNull() |
    df[DATE_COL].isin(INVALID_VALUES) | df[DATE_COL].isNull(), True).otherwise(False))

df = df.withColumn(DATE_COL, when(df[DATE_COL].isin(INVALID_VALUES) | df[DATE_COL].isNull(), most_common_date).otherwise(df[DATE_COL]))
df = df.filter(~(df[STORE_COL].isin(INVALID_VALUES) | df[STORE_COL].isNull()))
df = df.filter(~(df[PRODUCT_COL].isin(INVALID_VALUES) | df[PRODUCT_COL].isNull()))
df = df.withColumn(QUANTITY_COL, when(df[QUANTITY_COL].isin(INVALID_VALUES) | df[QUANTITY_COL].isNull(), avg_quantity).otherwise(df[QUANTITY_COL]))
df = df.withColumn(REVENUE_COL, when(df[REVENUE_COL].isin(INVALID_VALUES) | df[REVENUE_COL].isNull(), avg_revenue).otherwise(df[REVENUE_COL]))
df = df.withColumn(INSERT_DATE_COL, current_timestamp())

df = df.dropDuplicates()

q1_qty, q3_qty = df.selectExpr(
    f"percentile_approx({QUANTITY_COL}, 0.25)",
    f"percentile_approx({QUANTITY_COL}, 0.75)").first()
iqr_qty = q3_qty - q1_qty
low_qty, high_qty = q1_qty - 1.5 * iqr_qty, q3_qty + 1.5 * iqr_qty

df = df.filter((col(QUANTITY_COL) >= low_qty) & (col(QUANTITY_COL) <= high_qty))

q1_rev, q3_rev = df.selectExpr(
    f"percentile_approx({REVENUE_COL}, 0.25)",
    f"percentile_approx({REVENUE_COL}, 0.75)").first()
iqr_rev = q3_rev - q1_rev
low_rev, high_rev = q1_rev - 1.5 * iqr_rev, q3_rev + 1.5 * iqr_rev

df = df.filter((col(REVENUE_COL) >= low_rev) & (col(REVENUE_COL) <= high_rev))

df = df.withColumn(DATE_COL, col(DATE_COL).cast("timestamp")) \
       .withColumn(STORE_COL, col(STORE_COL).cast("int")) \
       .withColumn(PRODUCT_COL, col(PRODUCT_COL).cast("string")) \
       .withColumn(QUANTITY_COL, col(QUANTITY_COL).cast("int")) \
       .withColumn(REVENUE_COL, col(REVENUE_COL).cast("double"))

df.show()
df.printSchema()

df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://postgres-db:5432/processed_data") \
    .option("dbtable", "schema.processed_csv") \
    .option("user", "postgres") \
    .option("password", "casa1234") \
    .save()
