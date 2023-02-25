import pandas as pd
import numpy  as np
from shapely import wkb

class Dimensions:
    def __init__(self, datalake, workdir, engine=None):
        self.datalake = datalake
        self.workdir  = workdir
        self.engine = engine
        self.bronze_segments_file_path = f'{self.workdir}/bronze/segment.parquet'
        self.bronze_vertices_file_path = f'{self.workdir}/bronze/vertice.parquet'
        self.silver_segment_file_path = f'{self.workdir}/silver/segment.parquet'
        self.silver_vertice_file_path = f'{self.workdir}/silver/vertice.parquet'
        self.silver_historic_date_file_path = f'{self.workdir}/silver/model_historic_dates.parquet'

    def get_segments_from_bronze(self):
        if self.datalake.verify_file_exists(self.bronze_segments_file_path):
            return self.datalake.read_parquet_file(self.bronze_segments_file_path)
        else:
            return pd.DataFrame()

    def get_vertices_from_bronze(self):
        if self.datalake.verify_file_exists(self.bronze_vertices_file_path):
            return self.datalake.read_parquet_file(self.bronze_vertices_file_path)
        else:
            return pd.DataFrame()

    def get_segments_from_silver(self):
        if self.datalake.verify_file_exists(self.silver_segment_file_path):
            return self.datalake.read_parquet_file(self.silver_segment_file_path)
        else:
            return pd.DataFrame()

    def get_vertices_from_silver(self):
        if self.datalake.verify_file_exists(self.silver_vertice_file_path):
            return self.datalake.read_parquet_file(self.silver_vertice_file_path)
        else:
            return pd.DataFrame()

    def get_historic_date_from_silver(self):
        if self.datalake.verify_file_exists(self.silver_historic_date_file_path):
            return self.datalake.read_parquet_file(self.silver_historic_date_file_path)
        else:
            return pd.DataFrame()

    def get_districts_from_database(self):
        districts = pd.read_sql_query('select * from "map_district"',con=self.engine)
        districts['geometry'] = districts['geometry'].apply(lambda x : np.NaN if x == None else wkb.loads(x, hex=True))
        districts['name']     = districts['name'].apply(lambda x : "default" if x == None else x)
        districts             = districts.rename(columns={"district_id" : "id"})
        return districts.sort_values(by=['id'])
    
    def get_neighborhoods_from_database(self):
        neighborhoods = pd.read_sql_query('select * from "map_neighborhood"',con=self.engine)
        neighborhoods['geometry'] = neighborhoods['geometry'].apply(lambda x : np.NaN if x == None else wkb.loads(x, hex=True))
        neighborhoods['name']     = neighborhoods['name'].apply(lambda x : "default" if x == None else x)
        neighborhoods             = neighborhoods.rename(columns={"neighborhood_id" : "id"})
        return neighborhoods.sort_values(by=['id'])

    def get_zones_from_database(self):
        zones = pd.read_sql_query(
            'select z.zone_id, z.name, a.geometry, z.priority \
                FROM "map_zones" z \
                JOIN "map_areas" a on z.area_id = a.area_id \
             WHERE z.status = True \
             AND a.status = True',con=self.engine)
        zones['geometry'] = zones['geometry'].apply(lambda x : np.NaN if x == None else wkb.loads(x, hex=True))
        zones['name']     = zones['name'].apply(lambda x : "default" if x == None else x)
        zones             = zones.rename(columns={"zone_id" : "id"})
        return zones.sort_values(by=['priority'], ascending=True)

    def filter_default_value(self, dataframe):
        default_value = dataframe[~(dataframe['id'] == 0)]
        return default_value
