import pandas as pd

from hurricane.utils.datalake import Datalake
from hurricane.data.extract.interfaces import Interface as ExtractInterface
from hurricane.data.transform.interfaces import Interface as TransformInterface

class Interface:
    def process_raw_interface(**args):
        dag_id      = args['dag_id']
        adl         = args['adl']
        workdir     = args['workdir']
        name        = args['interface_name']
        config      = args['config_json']
        datalake    = Datalake(adl, dag_id)
        extract     = ExtractInterface(datalake, workdir, name)
        transform   = TransformInterface(datalake, workdir, name, config)

        df_raw, raw_files = extract.from_raw()

        if not df_raw.empty:
            df_bronze = extract.from_bronze()
            df_interface = transform.process(df_raw, dag_id)
            new_df_bronze = pd.concat([df_bronze, df_interface]).drop_duplicates(
                subset=['KEY'],
                ignore_index=True,
                keep='last'
            ).reset_index(drop=True)
            datalake.write_parquet_file_from_dataframe(extract.bronze_file_path, new_df_bronze)
        
        # Backup raw files
        for file in raw_files:
            print(f'Move the raw file [{file}] to historic directory [{extract.historic_path}]')
            datalake.move_file_to_historic(file, extract.historic_path, suffix = "", append_date=True)