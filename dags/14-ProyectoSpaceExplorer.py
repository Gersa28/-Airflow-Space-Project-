'''
Proyecto: Explorar el Espacio
Los analistas necesitan la información de quienes han accedido 
al satélite e información del historial de eventos de SpaceX:

Procesamiento

1. Esperar a que la NASA nos dé autorización para acceder a los datos del satélite.

2. Recolectar datos del satélite y dejarlos en un fichero.

3. Recolectar datos de la API de SpaceX y dejarlos en un fichero.

4. Enviar un mensaje a los equipos de que los datos finales están disponibles.
'''

import pandas as pd

from airflow import  DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.operators.email import EmailOperator

def _generate_data(**kwargs):

    data = pd.DataFrame({"Student": ["Maria Cruz", "Daniel Crema","Elon Musk", "Karol Castrejon", "Freddy Vega","Felipe Duque"],
        "timestamp": [kwargs['logical_date'],kwargs['logical_date'], 
                    kwargs['logical_date'], kwargs['logical_date'],
                    kwargs['logical_date'],kwargs['logical_date']]})
    data.to_csv(f"/tmp/generate_data_{kwargs['ds_nodash']}.csv",header=True, index=False)

with DAG(dag_id="14-SpaceExplorer",
         description="Comando bash para simular la respuesta de confirmación de la NASA:",
         start_date=datetime(2023, 1, 1)) as dag:

    task_1 = BashOperator(task_id = "Respuesta_Confirmacion_NASA",
                    bash_command='sleep 20 && echo "Confirmación de la NASA, pueden proceder" > /tmp/response_{{ds_nodash}}.txt')
    
    task_1_1 = BashOperator(task_id = "Leer_Datos_Respuesta_Nasa",
                    bash_command='ls /tmp && head /tmp/response_{{ds_nodash}}.txt')
    
    task_2 = BashOperator(task_id = "Obtener_Datos_SPACEX",
                    bash_command="curl https://api.spacexdata.com/v4/launches/past > /tmp/spacex_{{ds_nodash}}.json")
    
    task_3 = PythonOperator(task_id="Respuesta_Satelite",
                    python_callable=_generate_data)
    
    task_4 = BashOperator(task_id = "Leer_Datos_Respuesta_Satelite",
                    bash_command='ls /tmp && head /tmp/generate_data_{{ds_nodash}}.csv')

    email_analistas = EmailOperator(task_id='Notificar_Analistas',
                    to = "gersalina28@gmail.com",
                    subject = "Notificación Datos finales disponibles",
                    html_content = "Notificación para los analistas. Los datos finales están disponibles",
                    dag = dag)                 
    
    task_1 >> task_1_1 >> task_2 >> task_3 >> task_4 >> email_analistas