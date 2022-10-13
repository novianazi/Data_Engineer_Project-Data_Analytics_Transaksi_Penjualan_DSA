import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from data import appDump

with DAG{
    dag_id = "final_project_airflow",
    start_date=datetime.datetime(2022, 8, 14),
    schedule_interval=None,
    catchup=False,    
}as dag:
    insert_dataset_to_dwh = PythonOperator(
        task_id = "insert_dataset_to_dwh"
        python_callable=appDump.__main__
    )
