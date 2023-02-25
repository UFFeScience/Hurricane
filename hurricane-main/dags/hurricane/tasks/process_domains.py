from hurricane.utils.datalake import Datalake
from hurricane.data.extract.infos import Infos as ExtractInfos
from hurricane.data.transform.domains import Domains as TransformDomains

import pandas as pd

class Domains:
    def process_bronze_time_domain(**args):
        dag_id   = args['dag_id']
        adl      = args['adl']
        workdir  = args['workdir']
        config   = args['config_json']
        datalake = Datalake(adl, dag_id)
        info     = ExtractInfos(datalake, workdir, config)
        domain   = TransformDomains(datalake, workdir)

        df_infos = info.get_infos_from_bronze()
        df_times = domain.get_times(df_infos)
                
        datalake.write_parquet_file_from_dataframe(domain.bronze_time_file_path, df_times)

    def process_silver_time_domain(**args):
        dag_id   = args['dag_id']
        adl      = args['adl']
        workdir  = args['workdir']
        datalake = Datalake(adl, dag_id)
        domain   = TransformDomains(datalake, workdir)

        df_bronze_times = domain.get_times_from_bronze()
        df_silver_times = domain.get_times_from_silver()

        if df_silver_times is None or df_silver_times.empty:
            time_dimension = domain.generate_id(df_bronze_times, 'time_id')
            datalake.write_parquet_file_from_dataframe(domain.silver_time_file_path, time_dimension)
        else:
            next_id = ( df_silver_times['time_id'].max() + 1 )
            silver_compare = df_silver_times.loc[:, df_silver_times.columns != 'time_id']
            diff = df_bronze_times.merge(silver_compare, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='left_only']
            partial_dimension = domain.generate_id(diff, 'time_id', next_id)
            time_dimension = pd.concat([df_silver_times, partial_dimension])
            time_dimension = time_dimension.drop(['_merge'], errors='ignore', axis=1)
            datalake.write_parquet_file_from_dataframe(domain.silver_time_file_path, time_dimension)
