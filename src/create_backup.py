import boto3
import logging
import fastavro
import os
from sqlalchemy import create_engine
import pandas as pd
from get_info_poc import *
from datetime import datetime

logging.basicConfig(level=logging.INFO)

s3_bucket = "pocbucket9876"
host = get_parameter_poc(parameter_name="db-host")
user, password = get_secret_poc(secret_name="POC_DB_Credentials")
database = 'poc'

def backup_table(table_name, campos):
    
    try: 
        logging.info(f"Iniciando backup para la tabla: {table_name}")

        engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")
        df = pd.read_sql(f"SELECT * FROM {database}.{table_name}", con=engine)
        if "hire_datetime" in df.columns:
            df["hire_datetime"] = df["hire_datetime"].astype(str)

        for col in ["department_id", "job_id"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x)).astype("Int64")
        for col in ["name", "hire_datetime"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x)).astype(object)

        engine.dispose()

        if df.empty:
                logging.warning(f"Table '{table_name}' vacia. No se hace el backup")
                return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        avro_filename = f"{table_name}_backup_{timestamp}.avro"
        local_folder = f"../data/backups/{table_name}/"
        local_path = f"../data/backups/{table_name}/{avro_filename}"
        os.makedirs(local_folder, exist_ok=True)

        schema = {
        "type": "record",
        "name": f"{table_name}",
        "fields": campos
        }

        records = df.to_dict(orient="records")
        with open(local_path, "wb") as f:
            fastavro.writer(f, schema, records)
        logging.info(f"Avro creado en: {local_path}")

        s3_key = f"backups/{table_name}/{avro_filename}"
        s3_client = boto3.client("s3")
        s3_client.upload_file(local_path, s3_bucket, s3_key)
        logging.info(f"Backup subido a S3: s3://{s3_bucket}/{s3_key}")
    
    except Exception as e:
        logging.error(f"Error en el backup de {table_name}: {e}")


backup_table(table_name="jobs", campos=[{"name": "id", "type": "int"},
                                        {"name": "job", "type": "string"}])
backup_table(table_name="departments", campos=[{"name": "id", "type": "int"}, 
                                               {"name": "department", "type": "string"}])
                                              
backup_table(table_name="hired_employees", campos=[{"name": "id", "type": "int"}, 
                                                   {"name": "name", "type": ["null", "string"]}, 
                                                   {"name": "hire_datetime", "type": "string"},
                                                   {"name": "department_id", "type": ["null", "int"]},
                                                   {"name": "job_id", "type": ["null", "int"]}])
