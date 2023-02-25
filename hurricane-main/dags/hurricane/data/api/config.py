import json
from hurricane.utils.airflow_variable import AirflowVariable
from hurricane.utils.datalake import Datalake

class Config:
    def __init__(self, adl, dag_id, related_dag_id=None):
        self.dag_id         = dag_id
        self.adl            = adl
        self.related_dag_id = related_dag_id
        self.config_path    = AirflowVariable().get_variable('hurricane_config_path')

    def get_related_config(self):
        datalake     = Datalake(self.adl, None)
        config_files = datalake.list_dir(self.config_path)
        config       = None
        if len(config_files) > 0:
            for file in config_files:
                with open(file, 'r') as json_file:
                    config = json.load(json_file)
                    if "places" in config and self.related_dag_id == config['dag_id']:
                        break
                    else:
                        config = None
        if config is None: raise Exception(f"Config not found by DagId {self.related_dag_id}")
        return config

    def get_configs(self):
        datalake     = Datalake(self.adl, None)
        config_files = datalake.list_dir(self.config_path)
        info_config  = None
        model_config = None

        if len(config_files) > 0:
            for file in config_files:
                with open(file, 'r') as json_file:
                    info_config = json.load(json_file)
                    if "raw_interfaces" in info_config and info_config['dag_id'] == self.dag_id:
                        break
                    else:
                        info_config = None
        
            if info_config is not None:
                for file in config_files:
                    with open(file, 'r') as json_file:
                        model_config = json.load(json_file)
                        if "places" in model_config and model_config['dag_id'] == info_config['related_dag_id']:
                            break
                        else:
                            model_config = None

        return model_config, info_config

    def get_dependency_configs(self):
            datalake     = Datalake(self.adl, None)
            config_files = datalake.list_dir(self.config_path)
            dependency_configs  = []
            
            if len(config_files) > 0:
                for file in config_files:
                    with open(file, 'r') as json_file:
                        info_config = json.load(json_file)
                        if "raw_interfaces" in info_config and info_config['workflow_type'].upper() == 'GENERAL' and info_config['related_dag_id'] == self.dag_id:
                            dependency_configs.append(info_config)

            return dependency_configs