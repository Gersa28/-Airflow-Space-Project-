from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="06-1-orquestacion",
         description="Probando la orquestacion",
         schedule="@daily",
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 6, 1),
         default_args={"depends_on_past": True}, # Cada tarea depende del éxito de la anterior
         max_active_runs=1 # Solo se ejecuta un día a la vez
         ) as dag:

    t1 = BashOperator(task_id="tarea1",
                      bash_command="sleep 2 && echo 'Tarea 1'") # Demorar 2 Segundos

    t2 = BashOperator(task_id="tarea2",
                      bash_command="sleep 2 && echo 'Tarea 2'")

    t3 = BashOperator(task_id="tarea3",
                      bash_command="sleep 2 && echo 'Tarea 3'")

    t4 = BashOperator(task_id="tarea4",
                      bash_command="sleep 2 && echo 'Tarea 4'")

    t1 >> t2 >> [t3,t4]