from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from hurricane.utils.airflow_variable import AirflowVariable
from hurricane.tasks.process_generate_heuristic_files import Heuristic
from sqlalchemy import create_engine

env = AirflowVariable()

dag_id   = 'HURRICANE-Heuristc-File'
adl      = None
workdir  = env.get_variable('crime_workdir')

default_args = {
    'owner': 'PMERJ/UFF',
    'depends_on_past': False,
    "email": ["maiconbanni@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2021, 4, 1)
}

with DAG(
    dag_id,
    default_args=default_args,
    catchup=False,
    schedule_interval = "@once"
) as dag:
    
    process_generate_heuristic_files = PythonOperator(
        dag = dag,
        task_id = 'process_generate_heuristic_files',
        python_callable = Heuristic.process_generate_heuristic_files,
        op_kwargs = {
            'dag_id' : 'Hurricane-PMERJ-Crimes',
            'adl'    : adl
        }
    )

process_generate_heuristic_files