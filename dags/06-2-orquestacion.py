from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG(dag_id="06-2-orquestacion",
         description="Probando la orquestacion",
         schedule="0 7 * * 1", # Los Lunes a las 7 A.M.
         start_date=datetime(2022, 5, 1),
         end_date=datetime(2022, 6, 1),
         default_args={"depends_on_past": True}, # Cada tarea depende del éxito de la anterior
         max_active_runs=1 # Solo se ejecuta un día a la vez
         ) as dag:

    t1 = EmptyOperator(task_id="tarea1") 

    t2 = EmptyOperator(task_id="tarea2")

    t3 = EmptyOperator(task_id="tarea3")

    t4 = EmptyOperator(task_id="tarea4")

    t1 >> t2 >> t3 >> t4