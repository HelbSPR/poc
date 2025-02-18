# Funciones auxiliares usadas para:
# Extraer el endpoint de la base de datos, que está almacenado en SSM Parameter Store
# Extraer las credenciales de acceso a la base de datos, que están almacenadas en Secrets Manager

import boto3
import json
from botocore.exceptions import ClientError

def get_secret_poc(secret_name, region_name="us-east-1"):

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    user = secret_dict.get("user")
    password = secret_dict.get("password")
    return user, password


def get_parameter_poc(parameter_name, region_name="us-east-1"):
    
    session = boto3.session.Session()
    client = session.client(
        service_name='ssm',
        region_name=region_name
    )
    try:
        response = client.get_parameter(
            Name=parameter_name 
        )
    except ClientError as e:
        raise e
    
    parameter_value = response['Parameter']['Value']
    return parameter_value

