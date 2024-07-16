from airflow import DAG
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
#---------------------------------------------------------------------
# Estandar Constructor
my_dag1 = DAG(
    dag_id="My_first_dag",
    description="This is my first DAG",
    start_date=datetime(2022, 10, 31),
    schedule="@daily",
    catchup=False,
)
op1 = EmptyOperator(task_id="My_first_task", dag=my_dag1)

#---------------------------------------------------------------------
# Context Manager
with DAG(
    dag_id="MY_Second_DAG",
    description="Esto es el segundo DAG",
    start_date=datetime(2023, 1, 11),
    schedule="@once",
) as dag2:
    op = EmptyOperator(task_id="My_second_task")

#---------------------------------------------------------------------
# Decorators
@dag(
    dag_id="My_third_DAG",
    description="Tercer Dag con Decorators",
    start_date=datetime(2023, 1, 11),
    schedule="0 0 * * *",
    catchup=False,
)
def generateDAG():
    op = EmptyOperator(task_id="My_third_task")

my_dag3 = generateDAG()