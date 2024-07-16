from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def print_hello():
    print('Hello people from python funtion')
    
with DAG(
    dag_id='Dependencias',
    description='Creando dependencias entre tareas',
    schedule='@once',
    start_date=datetime(2022,8,1)
    ) as dag:
    
    t1 = PythonOperator(
        task_id = 'Tarea_1',
        python_callable = print_hello # Acá se llamará a la función de python
    )
    
    t2 = BashOperator(
        task_id = 'Tarea_2',
        bash_command = "echo 'Tarea_2 Realizada'" # Acá se llamará a la función de python
    )
    
    t3 = BashOperator(
        task_id = 'Tarea_3',
        bash_command = "echo 'Tarea_3 Realizada'" # Acá se llamará a la función de python
    )
    
    t4 = BashOperator(
        task_id = 'Tarea_4',
        bash_command = "echo 'Tarea_4 Realizada'" # Acá se llamará a la función de python
    )
    
# Creareamos las dependencias entre las tareas
# t1.set_downstream(t2) 
# t2.set_downstream([t3,t4])  

# Otra manera
t1 >> t2 >> [t3,t4]