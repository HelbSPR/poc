import boto3
import logging
import fastavro
import os
from sqlalchemy import create_engine,text
import pandas as pd
from get_info_poc import *
from datetime import datetime

logging.basicConfig(level=logging.INFO)

s3_bucket = "pocbucket9876"
host = get_parameter_poc(parameter_name="db-host")
user, password = get_secret_poc(secret_name="POC_DB_Credentials")
database = 'poc'


def restore_table_from_backup(table_name, avro_filename):

    try:
        logging.info(f"Iniciando restauracion para la tabla: {table_name}")

        local_folder = f"../data/restores/{table_name}/"
        local_path = f"../data/restores/{table_name}/{avro_filename}"
        os.makedirs(local_folder, exist_ok=True)
        
        s3_key = f"backups/{table_name}/{avro_filename}"
        s3_client = boto3.client("s3")
        s3_client.download_file(s3_bucket, s3_key, local_path)
        logging.info(f"Archivo  Avro descargado de S3: {s3_key}")

        with open(local_path, "rb") as f:
            reader = fastavro.reader(f)
            records = [r for r in reader]  
        df = pd.DataFrame(records)

        if "hire_datetime" in df.columns:
            df["hire_datetime"] = pd.to_datetime(df["hire_datetime"],errors="coerce")

        df = df.where(pd.notna(df), None)

        # Debido a que las PK de algunas tablas son FK en otras tablas, es necesario eliminar la restricción
        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
        with engine.begin() as connection:
            connection.execute(text("SET FOREIGN_KEY_CHECKS=0;"))
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            df.to_sql(table_name, con=engine, if_exists="replace", index=False)
            connection.execute(text("SET FOREIGN_KEY_CHECKS=1;"))
        engine.dispose()
        
        logging.info(f"Restauracion exitosa de la tabla: {table_name}")

    except Exception as e:
        logging.error(f"Error en la restauracion de la tabla {table_name}: {e}")


restore_table_from_backup(table_name="jobs", avro_filename="jobs_backup_20250218_233516.avro")
restore_table_from_backup(table_name="departments", avro_filename="departments_backup_20250218_233517.avro")
restore_table_from_backup(table_name="hired_employees", avro_filename="hired_employees_backup_20250219_011554.avro")


