import json

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from hurricane.data.api.config import Config
from hurricane.utils.airflow_variable import AirflowVariable
from hurricane.utils.datalake import Datalake
from hurricane.schemas.tasks.raw_validations import RawValidations
from hurricane.tasks.process_raw_interface import Interface
from hurricane.tasks.process_domains import Domains
from hurricane.tasks.process_infos import Infos
from hurricane.tasks.process_postgresql_synchronization import SyncPostgreSQL

env          = AirflowVariable()
config_path  = env.get_variable('hurricane_config_path')
datalake     = Datalake(None, None)
config_files = datalake.list_dir(config_path)

def create_dag(config, related_config):
    number_partitions = 10
    dag = DAG(
        dag_id = config['dag_id'],
        schedule_interval = config['schedule_interval'],
        catchup=False,
        concurrency= config['max_concurrency'],
        default_args = {
            'owner': config['owner'] if config['owner'] else "HURRICANE",
            'depends_on_past': False,
            "email": config['email'],
            "email_on_failure": False,
            "email_on_retry": False,
            'retries': config['retries'],
            'max_active_runs' : 1,
            'start_date': datetime(2021, 1, 1)
        }
    )

    with dag:

        raw_validation = PythonOperator(
            task_id = 'process_raw_validations',
            python_callable=RawValidations.process_raw_validations,
            op_kwargs = {
                'dag_id'        : config['dag_id'],
                'adl'           : config['datalake_client'],
                'workdir'       : config['datalake_workdir'],
                'config_json'   : config['raw_interfaces']
            }
        )

        raw_interface_process = []
        for raw in config['raw_interfaces']:
            task = PythonOperator(
                task_id='process_raw_{}'.format(raw['name']),
                python_callable=Interface.process_raw_interface,
                op_kwargs = {
                    'dag_id'        : config['dag_id'],
                    'adl'           : config['datalake_client'],
                    'workdir'       : config['datalake_workdir'],
                    'interface_name': raw['name'],
                    'config_json'   : raw
                }
            )
            raw_interface_process.append(task)
            raw_validation.set_downstream(task)

        process_bronze_time_domain = PythonOperator(
            dag = dag,
            task_id = 'process_bronze_time_domain',
            python_callable = Domains.process_bronze_time_domain,
            op_kwargs = {
                'dag_id'      : config['dag_id'],
                'adl'         : config['datalake_client'],
                'workdir'     : config['datalake_workdir'],
                'config_json' : config
            }
        )

        for p in raw_interface_process:
            process_bronze_time_domain.set_upstream(p)

        process_silver_time_domain = PythonOperator(
            dag = dag,
            task_id = 'process_silver_time_domain',
            python_callable = Domains.process_silver_time_domain,
            op_kwargs = {
                'dag_id'  : config['dag_id'],
                'adl'     : config['datalake_client'],
                'workdir' : config['datalake_workdir']
            }
        )

        process_bronze_time_domain.set_downstream(process_silver_time_domain)

        process_check_model_and_info = PythonOperator(
            task_id = f"process_check_model_and_info",
            python_callable = Infos.process_check_model_and_info,
            provide_context=True,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'number_partitions'     : number_partitions,
                'config_json'           : config,
                'related_config_json'   : related_config
            }
        )

        process_silver_time_domain.set_downstream(process_check_model_and_info)

        process_info = PythonOperator(
            task_id = f"process_{config['fact_tablename']}",
            python_callable = Infos.process_infos,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'number_partitions'     : number_partitions,
                'config_json'           : config
            }
        )

        info_silver_tasks = []
        for i in range(1, number_partitions):
            partition = str(i)
            info_task_partition = PythonOperator(
                task_id = f"process_{config['fact_tablename']}_partition_{partition}",
                python_callable=Infos.process_silver_infos,
                op_kwargs = {
                    'dag_id'                : config['dag_id'],
                    'adl'                   : config['datalake_client'],
                    'workdir'               : config['datalake_workdir'],
                    'partition'             : partition,
                    'config_json'           : config,
                    'related_config_json'   : related_config
                }
            )
            info_silver_tasks.append(info_task_partition)
            process_check_model_and_info.set_downstream(info_task_partition)
            info_task_partition.set_downstream(process_info)

        sync_vertice_in_postgresql = PythonOperator(
            task_id = f"sync_vertice_in_postgresql",
            python_callable = SyncPostgreSQL.build_table,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'config_json'           : config,
                'related_config_json'   : related_config,
                'tablename'             : "vertice"
            }
        )
        
        process_info.set_downstream(sync_vertice_in_postgresql)

        sync_segment_in_postgresql = PythonOperator(
            task_id = f"sync_segment_in_postgresql",
            python_callable = SyncPostgreSQL.build_table,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'config_json'           : config,
                'related_config_json'   : related_config,
                'tablename'             : "segment"
            }
        )

        process_info.set_downstream(sync_segment_in_postgresql)

        sync_time_in_postgresql = PythonOperator(
            task_id = f"sync_time_in_postgresql",
            python_callable = SyncPostgreSQL.build_table,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'config_json'           : config,
                'related_config_json'   : related_config,
                'tablename'             : "time"
            }
        )

        process_info.set_downstream(sync_time_in_postgresql)

        sync_info_in_postgresql = PythonOperator(
            task_id = f"sync_info_in_postgresql",
            python_callable = SyncPostgreSQL.build_table,
            op_kwargs = {
                'dag_id'                : config['dag_id'],
                'adl'                   : config['datalake_client'],
                'workdir'               : config['datalake_workdir'],
                'config_json'           : config,
                'related_config_json'   : related_config,
                'tablename'             : config['fact_tablename']
            }
        )

        process_info.set_downstream(sync_info_in_postgresql)
    
    return dag


if len(config_files) > 0:
    for file in config_files:
        with open(file, 'r') as json_file:
            config = json.load(json_file)
            if "workflow_type" in config and config['workflow_type'].upper() == "GENERAL":
                related_config = Config(datalake, config['dag_id'], config['related_dag_id']).get_related_config()
                globals()[config['dag_id']] = create_dag(config, related_config)

