from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from HelloOperator import HelloOperator

def print_hello():
    print('Hello people from python funtion')
    
with DAG(
    dag_id='CustomOperator',
    description='Primer operador personalizado',
    schedule_interval='@once',
    start_date=datetime(2022,8,1)
    ) as dag:
    
    t1 = HelloOperator(task_id = "Saludos", name= "Ger") # Imprime nombre recibido por parámetro 

