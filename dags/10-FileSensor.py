from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


from airflow.sensors.filesystem import FileSensor


with DAG(dag_id="10-Filesensor",
    description="FileSensor",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 20),
    end_date=datetime(2022, 8, 25),
    max_active_runs=1
) as dag:

    t1 = BashOperator(task_id="creating_file",
					  bash_command="sleep 10 && pwd && touch /tmp/file.txt") # Se creará en el Worker

    t2 = FileSensor(task_id="waiting_file",
					  filepath="/tmp/file.txt") # Ruta que se estará sensando

    t3 = BashOperator(task_id="end_task",
					  bash_command="echo 'El fichero ha llegado'")

    t1 >> t2 >> t3