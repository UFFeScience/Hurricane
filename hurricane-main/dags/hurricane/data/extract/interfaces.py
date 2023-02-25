import pandas as pd

class Interface:
    def __init__(self, datalake, workdir, name):
        self.datalake         = datalake
        self.workdir          = workdir
        self.name             = name
        self.raw_path         = f'{workdir}/raw/{self.name}'
        self.historic_path    = f'{workdir}/historic/{self.name}'
        self.bronze_file_path = f'{workdir}/bronze/{self.name}.parquet'

    def from_bronze(self):
        dataframe = pd.DataFrame()
        if self.datalake._verify_if_path_if_exists(self.bronze_file_path):
            dataframe = self.datalake.read_parquet_file(self.bronze_file_path)
        return dataframe

    def from_raw(self):
        files = self.datalake.list_dir(self.raw_path)        
        df_raw_interface = pd.DataFrame()
        for file in files:
            print(f'Get data from raw file [{file}]')
            dataframe = self.datalake.read_csv_file_on_dataframe(file, encoding = "utf-16", engine = 'python', header = 0)
            df_raw_interface = df_raw_interface.append(dataframe)
        
        return df_raw_interface, files