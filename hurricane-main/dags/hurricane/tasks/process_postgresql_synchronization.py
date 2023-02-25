from hurricane.utils.datalake import Datalake
from hurricane.data.api.sync import SyncDatabase
from hurricane.data.extract.dimensions import Dimensions as ExtractDimensions
from hurricane.data.extract.infos import Infos as ExtractInfos
from hurricane.data.transform.domains import Domains as TransformDomains
from airflow.exceptions import AirflowSkipException

import pandas as pd
import numpy as np

class SyncPostgreSQL:
    def build_table(**args):
        
        model_changed = args['ti'].xcom_pull(key="model_changed")
        print("model_changed: ", model_changed)
        dag_id         = args['dag_id']
        adl            = args['adl']
        config         = args['config_json']
        related_config = args['related_config_json']
        tablename      = args['tablename'].lower()
        datalake       = Datalake(adl, dag_id)
        
        if model_changed == False and tablename in ["segment", "vertice"]:
            raise AirflowSkipException

        interfaces = []
        schema     = related_config['schema']
        tablespace = related_config['tablespace']
        urlconnect = related_config['postgresql_url']

        # Read database file
        if tablename == "segment":
            extract   = ExtractDimensions(datalake, related_config['datalake_workdir'])
            dataframe = extract.get_segments_from_silver()
            file_path = extract.workdir + "/silver/"
        elif tablename == "vertice":
            extract   = ExtractDimensions(datalake, related_config['datalake_workdir'])
            dataframe = extract.get_vertices_from_silver()
            file_path = extract.workdir + "/silver/"
        elif tablename == "time":
            extract = TransformDomains(datalake, config['datalake_workdir'])
            dataframe = extract.get_times_from_silver()
            file_path = extract.workdir + "/silver/"
        else:
            extract = ExtractInfos(datalake, config['datalake_workdir'], config)
            dataframe = extract.get_infos_from_gold()
            file_path = extract.workdir + "/gold/"
            interfaces = [f"{v['name']}" for v in config['raw_interfaces']]

        sync = SyncDatabase(urlconnect, schema, tablename, tablespace)

        parquet_file_path = f"{file_path}{tablename}.parquet"
        dataframe = pd.read_parquet(parquet_file_path)

        create_table = sync.get_create_table(interfaces)
        sync.execute_sql(create_table)

        truncate_table = sync.get_truncate_table()
        sync.execute_sql(truncate_table)

        if tablename not in ["vertice", "segment", "time"]:
            table = sync.read_table()
            if not set(interfaces).issubset(set(table.columns)):
                drop_table = sync.get_drop_table()
                sync.execute_sql(drop_table)
                create_table = sync.get_create_table(interfaces)
                sync.execute_sql(create_table)
        else:
            for c in sync.integer_columns[tablename]: dataframe[c] = dataframe[c].astype(int)
        
        if tablename == "vertice":
            alter_table = f"alter table {schema}.{tablename} drop column if exists vertice"
            sync.execute_sql(alter_table)
            alter_table = f"alter table {schema}.{tablename} add column vertice text not null"
            sync.execute_sql(alter_table)
        elif tablename == "segment":
            dataframe['name']    = dataframe['name'].apply(lambda name : "{" + ",".join(name) + "}")
            dataframe['highway'] = dataframe['highway'].apply(lambda highway : "{" + ",".join(highway) + "}")
        elif tablename == "time":
            pass
        else:
            for c in dataframe.columns: dataframe[c] = dataframe[c].astype(int)
        
        csv_file_path = sync.generate_csv_file_path(file_path, dataframe)
        sql_copy = sync.get_copy_table_by_csv(csv_file_path, dataframe.columns)
        sync.execute_copy(sql_copy)

        if tablename == "vertice":
            sync.execute_sql("ALTER TABLE vertice add column geom geometry(point, 4326)")
            sync.execute_sql("UPDATE vertice SET geom = st_setsrid(st_makepoint(split_part(vertice, ',',1) ::double precision, split_part(vertice, ',',2)::double precision),4326)")
            sync.execute_sql("ALTER TABLE vertice drop column vertice")
            sync.execute_sql("ALTER TABLE vertice rename column geom to vertice")
        
        datalake.remove_file(csv_file_path)