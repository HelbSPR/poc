from fastapi import FastAPI, Depends, HTTPException, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime
import pandas as pd
import logging
import cryptography
from get_info_poc import *

#logging.basicConfig(level=logging.INFO)
logging.basicConfig(filename="../data/logs/transacciones_no_validas.log", level=logging.INFO, format="%(asctime)s - %(message)s")

host = get_parameter_poc(parameter_name="db-host")
user, password = get_secret_poc(secret_name="POC_DB_Credentials")
database = 'poc'

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}",pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

app = FastAPI()
templates = Jinja2Templates(directory="templates")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Modelo de datos del request
class Registro(BaseModel):
    name: str
    hire_datetime: str  
    department_id: int
    job_id: int

class RegistrosRequest(BaseModel):
    registros: list[Registro]

@app.post("/agregar-empleados")
async def agregar_empleados(request: RegistrosRequest, db: Session = Depends(get_db)):
    MAX_REGISTROS = 1000
    if len(request.registros) > MAX_REGISTROS:
        raise HTTPException(status_code=400, detail=f"Demasiados empleados a agregar. No se agregó ninguno. El maximo permitido es {MAX_REGISTROS}.")

    inserted_count = 0
    failed_count = 0
    for registro in request.registros:
        try:
            if not all([registro.name, registro.hire_datetime, registro.department_id, registro.job_id]):
                raise ValueError("Campos faltantes para la peticion")

            dep_check = db.execute(text("SELECT id FROM departments WHERE id = :dep_id"), {"dep_id": registro.department_id}).fetchone()
            job_check = db.execute(text("SELECT id FROM jobs WHERE id = :job_id"), {"job_id": registro.job_id}).fetchone()

            if not dep_check or not job_check:
                raise ValueError(f"Id del departamento {registro.department_id} or Id del job {registro.job_id} no validos")
            
            # Insert into MySQL
            sql = text("INSERT INTO hired_employees (name, hire_datetime, department_id, job_id) VALUES (:name, :hire_datetime, :department_id, :job_id)")
            db.execute(sql, {
                "name" : registro.name,
                "hire_datetime" : registro.hire_datetime,
                "department_id" : registro.department_id,
                "job_id" : registro.job_id
            })
            inserted_count += 1

        except Exception as e:
            failed_count += 1
            logging.info(f"Fallo para el registro: {registro.dict()} - Razon: {str(e)}")

    db.commit()  

    return {
        "Mensaje": "Procesamiento completado",
        "Insertados": inserted_count,
        "Fallidos": failed_count
    }

@app.get("/resumen_trimestral")
async def resumen_trimestral(request: Request, db: Session = Depends(get_db)):
    with db.connection() as conn: 
        consulta ='''SELECT 
                    t2.department,
                    t3.job,
                    COUNT(CASE WHEN QUARTER(t1.hire_datetime) = 1 THEN 1 END) AS Q1,
                    COUNT(CASE WHEN QUARTER(t1.hire_datetime) = 2 THEN 1 END) AS Q2,
                    COUNT(CASE WHEN QUARTER(t1.hire_datetime) = 3 THEN 1 END) AS Q3,
                    COUNT(CASE WHEN QUARTER(t1.hire_datetime) = 4 THEN 1 END) AS Q4
                    FROM poc.hired_employees t1
                    INNER JOIN poc.departments t2 ON t1.department_id = t2.id
                    INNER JOIN poc.jobs t3 ON t1.job_id = t3.id
                    WHERE YEAR(t1.hire_datetime) = 2021
                    GROUP BY t2.department, t3.job
                    ORDER BY t2.department, t3.job'''
        data_store = pd.read_sql(consulta, con=conn.connection)
    return templates.TemplateResponse("info_por_trimestre.html", {"request": request, "data": data_store.to_dict(orient="records")})

@app.get("/resumen_departamento")
async def resumen_departamento(request: Request, db: Session = Depends(get_db)):
    with db.connection() as conn: 
        consulta ='''WITH contrataciones_por_depto AS (
                SELECT 
                    t2.id,
                    t2.department,
                    COUNT(t1.id) AS hired
                FROM hired_employees t1
                JOIN departments t2 ON t1.department_id = t2.id
                WHERE YEAR(t1.hire_datetime) = 2021
                GROUP BY t2.id, t2.department
            ), contrataciones_promedio AS (
                SELECT AVG(hired) AS avg_contrataciones 
                FROM contrataciones_por_depto
            )
            SELECT 
                t3.id, 
                t3.department, 
                t3.hired
            FROM contrataciones_por_depto t3
            JOIN contrataciones_promedio t4 ON t3.hired > t4.avg_contrataciones
            ORDER BY t3.hired DESC'''
        data_store = pd.read_sql(consulta, con=conn.connection)
        print(data_store.head())
    return templates.TemplateResponse("info_por_departamento.html", {"request": request, "data": data_store.to_dict(orient="records")})
    