import psycopg2
import pandas as pd
import boto3

# Configuración de PostgreSQL
DB_CONFIG = {
    "host": "localhost",
    "database": "retail_db",
    "user": "postgres",
    "password": "casa1234",
    "port": 5432
}

# Configuración de LocalStack (S3 local)
S3_CONFIG = {
    "endpoint_url": "http://localhost:4566",  # URL de LocalStack
    "aws_access_key_id": "test",
    "aws_secret_access_key": "test",
}

BUCKET_NAME = "guille-bucket"
CSV_FILE = "./../data_bda/csv/postgres_data.csv"
S3_FOLDER = "Postgres/"

def export_db_to_csv():
    """Extrae datos de PostgreSQL y los guarda en un CSV."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT * FROM Stores"
        df = pd.read_sql(query, conn)
        df.to_csv(CSV_FILE, index=False)
        print(f"Datos exportados a {CSV_FILE}")
    except Exception as e:
        print(f"Error exportando datos: {e}")
    finally:
        if conn:
            conn.close()

def upload_to_s3():
    """Sube el CSV a una subcarpeta en un bucket de S3 en LocalStack."""
    try:
        s3_client = boto3.client("s3", **S3_CONFIG)

        existing_buckets = [b["Name"] for b in s3_client.list_buckets()["Buckets"]]
        if BUCKET_NAME not in existing_buckets:
            s3_client.create_bucket(Bucket=BUCKET_NAME)

        s3_client.upload_file(CSV_FILE, BUCKET_NAME, f"{S3_FOLDER}{CSV_FILE}")
        print(f"Archivo {CSV_FILE} subido a {BUCKET_NAME}/{S3_FOLDER}")
    except Exception as e:
        print(f"Error subiendo a S3: {e}")

if __name__ == "__main__":
    export_db_to_csv()
    upload_to_s3()
