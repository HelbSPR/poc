# Creacion de las tablas en la base de datos
# Carga de información histórica a la base de datos

import pymysql
import pandas as pd
import logging
from get_info_poc import *

logging.basicConfig(level=logging.INFO)

# Parametros de conexion
host = get_parameter_poc(parameter_name="db-host")
user, password = get_secret_poc(secret_name="POC_DB_Credentials")
database = 'poc'
    
def conectar_a_mysql(host, user, password, database, local_infile=False):
    try:
        connection = pymysql.connect(host=host, user=user, password=password, database=database,local_infile=local_infile)
        logging.info("Conexion establecida exitosamente.")
        return connection
    except pymysql.MySQLError as e:
        logging.info(f"Error conectandose a la base de datos: {e}")
        raise e

def ejecutar_consulta(conexion, consultas, proceso):
    try:
        with conexion.cursor() as cursor:
            for consulta in consultas:
                cursor.execute(consulta)
            conexion.commit()
            logging.info(f"{proceso}: Consultas ejecutadas exitosamente.")
    except pymysql.MySQLError as e:
        logging.error(f"Error ejecutando consulta: {e}")
        conexion.rollback()

def crear_tablas():
    
    consultas_creacion_tablas = [
        '''CREATE TABLE IF NOT EXISTS departments (
            id INT AUTO_INCREMENT PRIMARY KEY,
            department VARCHAR(255) NOT NULL
        )''',
        '''CREATE TABLE IF NOT EXISTS jobs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            job VARCHAR(255) NOT NULL
        )''',
        '''CREATE TABLE IF NOT EXISTS hired_employees (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255),
            hire_datetime DATETIME,
            department_id INT,
            job_id INT,
            FOREIGN KEY (department_id) REFERENCES departments(id),
            FOREIGN KEY (job_id) REFERENCES jobs(id)
        )'''
    ]
    return consultas_creacion_tablas

def cargar_datos_historicos():
    consultas_carga_historica = [
        '''LOAD DATA LOCAL INFILE '../data/historicos/jobs.csv'
            INTO TABLE poc.jobs
            FIELDS TERMINATED BY ',' 
            LINES TERMINATED BY '\r\n'
        ''',
        '''LOAD DATA LOCAL INFILE '../data/historicos/departments.csv'
            INTO TABLE poc.departments
            FIELDS TERMINATED BY ',' 
            LINES TERMINATED BY '\r\n'
        ''',
        '''LOAD DATA LOCAL INFILE '../data/historicos/hired_employees.csv'
            INTO TABLE poc.hired_employees
            FIELDS TERMINATED BY ',' 
            LINES TERMINATED BY '\r\n'
            (id, @name, @hire_datetime, @department_id, @job_id)
            SET 
            name = IF(@name = '', NULL, @name),
            hire_datetime = IF(@hire_datetime = '', NULL, @hire_datetime), 
            department_id = IF(@department_id = '', NULL, @department_id), 
            job_id = IF(@job_id = '', NULL, @job_id);
        '''
    ]
    return consultas_carga_historica


conexion = conectar_a_mysql(host, user, password, database,local_infile=True)
try:
    ejecutar_consulta(conexion,crear_tablas(),"CREACION_TABLAS")
    ejecutar_consulta(conexion,cargar_datos_historicos(),"CARGA_HISTORICO")
finally:
    conexion.close()