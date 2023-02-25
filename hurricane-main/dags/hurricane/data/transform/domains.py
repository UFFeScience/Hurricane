import pandas as pd

class Domains:
    def __init__(self, datalake, workdir):
        self.datalake = datalake
        self.workdir  = workdir
        self.days_of_week = ['Segunda-Feira', 'Terça-Feira', 'Quarta-Feira', 'Quinta-Feira', 'Sexta-Feira', 'Sábado', 'Domingo']
        self.bronze_time_file_path = f'{self.workdir}/bronze/time.parquet'
        self.silver_time_file_path = f'{self.workdir}/silver/time.parquet'
        
    def get_times(self, df):
        time_list = []
        df_occurrence_times = df[['PERIOD', 'DATE']].copy()
        df_occurrence_times.drop_duplicates(ignore_index=True, inplace=True)
        df_occurrence_times.dropna(how='any', inplace=True)
        for index, row in df_occurrence_times.iterrows():
            template = [{
                'period'    : row['PERIOD'],
                'weekday'   : self.days_of_week[row['DATE'].weekday()],
                'day'       : row['DATE'].day,
                'month'     : row['DATE'].month,
                'year'      : row['DATE'].year
            }]
            time_list.extend(template)
        df_times = pd.DataFrame(time_list)
        return df_times

    def generate_id(self, dataframe, column_name_id, start_value=1):
        df = dataframe.copy()
        df = df.reset_index(drop=True)
        if not dataframe.empty:
            df[column_name_id] = 0
            df[column_name_id] = df.apply(lambda line: line.index + start_value)
        return df
        
    def get_times_from_bronze(self):
        if self.datalake.verify_file_exists(self.bronze_time_file_path):
            return self.datalake.read_parquet_file(self.bronze_time_file_path)
        else:
            return pd.DataFrame()

    def get_times_from_silver(self):
        if self.datalake.verify_file_exists(self.silver_time_file_path):
            return self.datalake.read_parquet_file(self.silver_time_file_path)
        else:
            return pd.DataFrame()
