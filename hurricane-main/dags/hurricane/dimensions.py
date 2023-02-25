import json

from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sqlalchemy import create_engine

from hurricane.utils.airflow_variable import AirflowVariable
from hurricane.utils.datalake import Datalake
from hurricane.tasks.process_dimensions import Dimensions
from hurricane.data.api.config import Config

env          = AirflowVariable()
config_path  = env.get_variable('hurricane_config_path')
datalake     = Datalake(None, None)
config_files = datalake.list_dir(config_path)

def create_dag(config):
    engine = create_engine(config['postgresql_url'])
    dag = DAG(
        dag_id = config['dag_id'],
        schedule_interval = "@once",
        catchup=False,
        default_args = {
            'owner': config['owner'] if config['owner'] else "HURRICANE",
            'depends_on_past': False,
            "email": config['email'],
            "email_on_failure": False,
            "email_on_retry": False,
            'retries': config['retries'],
            'start_date': datetime(2021, 1, 1)
        }
    )

    with dag:
        process_create_segments = PythonOperator(
            dag = dag,
            task_id = 'process_create_segments',
            python_callable = Dimensions.process_create_segments,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'places'    : config['places'],
                'engine'    : engine
            }
        )

        process_create_vertices = PythonOperator(
            dag = dag,
            task_id = 'process_create_vertices',
            python_callable = Dimensions.process_create_vertices,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'engine'    : engine
            }
        )

        process_associate_zones = PythonOperator(
            dag = dag,
            task_id = 'process_associate_zones',
            python_callable = Dimensions.process_associate_zones,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'engine'    : engine
            }
        )

        process_associate_districts = PythonOperator(
            dag = dag,
            task_id = 'process_associate_districts',
            python_callable = Dimensions.process_associate_districts,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'engine'    : engine
            }
        )

        process_associate_neighborhoods = PythonOperator(
            dag = dag,
            task_id = 'process_associate_neighborhoods',
            python_callable = Dimensions.process_associate_neighborhoods,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'engine'    : engine
            }
        )

        process_merge_vertices_into_segments = PythonOperator(
            dag = dag,
            task_id = 'process_merge_vertices_into_segments',
            python_callable = Dimensions.process_merge_vertices_into_segments,
            op_kwargs = {
                'dag_id'    : config['dag_id'],
                'adl'       : config['datalake_client'],
                'workdir'   : config['datalake_workdir'],
                'engine'    : engine
            }
        )

        process_create_segments.set_downstream(process_create_vertices)
        process_create_vertices.set_downstream(process_associate_zones)
        process_associate_zones.set_downstream(process_associate_districts)
        process_associate_districts.set_downstream(process_associate_neighborhoods)
        process_associate_neighborhoods.set_downstream(process_merge_vertices_into_segments)
        
        dependency_configs = Config(datalake, config['dag_id']).get_dependency_configs()
        
        for info_dag in dependency_configs:
            trigger_target = TriggerDagRunOperator(
                task_id=f"TRIGGER_DAG_{info_dag['dag_id']}",
                trigger_dag_id=info_dag['dag_id'],
                execution_date='{{ ds }}',
                reset_dag_run=True,
                wait_for_completion=True
            )
            process_merge_vertices_into_segments.set_downstream(trigger_target)

        return dag

if len(config_files) > 0:
    for file in config_files:
        with open(file, 'r') as json_file:
            config = json.load(json_file)
            if "workflow_type" in config and config['workflow_type'].upper() == "MODEL":
                globals()[config['dag_id']] = create_dag(config)
