from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(dag_id="09-1-ExternalTaskSensor",
    description="DAG principal",
    schedule_interval="@daily",
    start_date=datetime(2022, 8, 10),
    end_date=datetime(2022, 8, 25)
) as dag:

    t1 = BashOperator(task_id="tarea_1",
                      bash_command="sleep 10 && echo 'DAG finalizado!'",
                      depends_on_past=True)