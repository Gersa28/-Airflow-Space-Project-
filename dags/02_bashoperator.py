from airflow import DAG
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='primer_dag_bash',      
    description="Probando bash operator",
    schedule='@once', 
    start_date=datetime(2024, 8, 1)) as dag:
    t1 = BashOperator(
                    task_id='primer_task_bash',
                    bash_command='echo "Hola mundo con Bash"'
        )