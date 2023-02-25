import os
import pandas as pd

class Infos:
    def __init__(self, datalake, workdir, config):
        self.datalake               = datalake
        self.workdir                = workdir
        self.config                 = config
        self.silver_columns_compare = ['KEY', 'DATE', 'PERIOD', 'LATITUDE', 'LONGITUDE', 'INFO']
        self.silver_path            = f"{workdir}/silver/"
        self.silver_historic_dates  = self.silver_path + "model_historic_dates.parquet"
        self.gold_path            = f"{workdir}/gold/"
        self.gold_info_file_path    = f"{workdir}/gold/{self.config['fact_tablename']}.parquet"
        self.files_infos            = [f"{self.workdir}/bronze/{v['name']}.parquet" for v in self.config['raw_interfaces']]
    
    def get_infos_from_bronze(self):
        df_infos = pd.DataFrame()
        for file_info in self.files_infos:
            type_info = os.path.basename(file_info.rsplit('.', 1)[0])
            if self.datalake._verify_if_path_if_exists(file_info):
                df = self.datalake.read_parquet_file(file_info)
                df['INFO'] = type_info
                df_infos = pd.concat([df_infos, df], ignore_index=True)
        return df_infos

    def get_infos_partition_from_bronze(self, partition):
        df_infos = pd.DataFrame()
        for file_info in self.files_infos:
            if self.datalake._verify_if_path_if_exists(file_info):
                type_info = os.path.basename(file_info.rsplit('.', 1)[0])
                df = self.datalake.read_parquet_file(file_info)
                df['LATITUDE']  = df['LATITUDE'].round(7).astype(float)
                df['LONGITUDE'] = df['LONGITUDE'].round(7).astype(float)
                df = df[df['LONGITUDE'].apply(lambda l: str(l).endswith(partition))]
                if not df.empty:
                    df['INFO'] = type_info
                    df_infos = pd.concat([df_infos, df], ignore_index=True)
        return df_infos.reset_index()

    def get_infos_partition_from_silver(self, split_key):
        dataframe = pd.DataFrame()
        filename  = self.get_infos_filename_partition(split_key)
        if self.datalake._verify_if_path_if_exists(filename):
            dataframe = self.datalake.read_parquet_file(filename)
        return dataframe

    def get_all_infos_from_silver(self, number_partitions):
        df_infos = pd.DataFrame()
        for i in range(1, int(number_partitions)):
            filename  = self.get_infos_filename_partition(i)
            if self.datalake._verify_if_path_if_exists(filename):
                df = self.datalake.read_parquet_file(filename)
                df_infos = pd.concat([df_infos, df], ignore_index=True)
        return df_infos.reset_index()
    
    def get_infos_filename_partition(self, partition):
        return self.silver_path + f"{self.config['fact_tablename']}_" + str(partition) + ".parquet"
    
    def get_infos_from_gold(self):
        if self.datalake.verify_file_exists(self.gold_info_file_path):
            return self.datalake.read_parquet_file(self.gold_info_file_path)
        else:
            return pd.DataFrame()

    def get_historic_dates_from_silver(self):
        dataframe = pd.DataFrame()
        if self.datalake._verify_if_path_if_exists(self.silver_historic_dates):
            dataframe = self.datalake.read_parquet_file(self.silver_historic_dates)
        return dataframe