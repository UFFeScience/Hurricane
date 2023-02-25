from numpy.lib.function_base import append
import pandas as pd
import numpy as np

from hurricane.schemas.files_schema import FilesSchema

class Interface:
    def __init__(self, datalake, workdir, name, config):
        self.workdir  = workdir
        self.datalake = datalake
        self.name     = name
        self.config   = config

    def _get_duplicated_key_columns(self):
        return self.config['duplicated_key']

    def process(self, dataframe, dag_id):
        # Read parquet file in datalake
        df_interface = dataframe.copy()
        
        # Transform fields by config 
        files_schema = FilesSchema(dag_id)

        # include even empty layout columns
        unrequired_columns = files_schema.get_unrequired_columns(self.config)
        for column in unrequired_columns:
            if column not in df_interface.columns:
                df_interface[column] = np.nan
        
        df_interface = files_schema.convert_columns_by_type(df_interface, self.config)
        
        filter_date      = df_interface[self.config['rules_columns']['date']].notna()
        filter_period    = df_interface[self.config['rules_columns']['period']].notna()
        filter_latitude  = df_interface[self.config['rules_columns']['latitude']].notna()
        filter_longitude = df_interface[self.config['rules_columns']['longitude']].notna()

        df_interface = df_interface[filter_date & filter_period & filter_latitude & filter_longitude].copy()

        # Remove duplicates rows by key 
        df_interface.drop_duplicates(subset=self._get_duplicated_key_columns(), inplace=True, ignore_index=True, keep='last')
        
        # Insert duplicated key into dataframe 
        df_interface['KEY'] = ""
        for index, row in df_interface.iterrows():
            df_interface.loc[index, 'KEY'] = "|".join(row[self._get_duplicated_key_columns()].astype(str))

        # Rename Fixed Colums - Model Rules
        df_interface.rename(
            columns={
                self.config['rules_columns']['date']      : "DATE",
                self.config['rules_columns']['period']    : "PERIOD",
                self.config['rules_columns']['latitude']  : "LATITUDE",
                self.config['rules_columns']['longitude'] : "LONGITUDE"
            }, inplace = True, errors='ignore'
        )

        # Rename Features Colums
        df_interface.rename(columns=self.config['feature_columns'], inplace = True, errors='ignore')

        return df_interface
