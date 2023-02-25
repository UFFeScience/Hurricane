import pandas as pd
import numpy as np
from shapely import wkb
import psycopg2
from sqlalchemy import create_engine
from hurricane.utils.datalake import Datalake

class SyncDatabase:
    def __init__(self, urlconnect, schema, tablename, tablespace):
        self.schema     = schema
        self.tablename  = tablename
        self.tablespace = tablespace
        self.urlconnect = urlconnect
        self.engine = create_engine(urlconnect)
        self.integer_columns = {
            "vertice" : ["ver_id", "zone_id", "district_id", "neighborhood_id"],
            "segment" : ["seg_id", "ver_id_star", "ver_id_final"],
            "time"    : ["time_id", "day", "month", "year"]
        }

    def check_params(self):
        if self.schema == "" or self.schema is None: schema = "public"
        if self.tablename == "" or self.tablename is None: raise Exception(f"Tablename not defined")
        else: tablename = self.tablename.upper()
        return schema, tablename

    def get_drop_table(self):
        return f"drop table {self.schema}.{self.tablename}"

    def get_truncate_table(self):
        return f"truncate table {self.schema}.{self.tablename}"

    def get_create_table(self, interfaces, feature_columns):
        if self.tablename == "segment":
            create_table = f"create table if not exists {self.schema}.{self.tablename}( seg_id integer NOT NULL, oneway boolean, name character varying(254), highway text[], length double precision, ver_id_star integer NOT NULL, ver_id_final integer NOT NULL )"
        elif self.tablename == "time":
            create_table = f"create table if not exists {self.schema}.{self.tablename}( time_id integer NOT NULL, period character varying(32), weekday character varying(16), day integer, month integer, year integer)"
        elif self.tablename == "vertice":
            create_table = f"create table if not exists {self.schema}.{self.tablename}( ver_id integer NOT NULL, vertice text NOT NULL, zone_id integer NOT NULL, district_id integer NOT NULL, neighborhood_id integer NOT NULL)"
        else:
            create_table = f"create table if not exists {self.schema}.{self.tablename}( time_id integer NOT NULL, seg_id integer NOT NULL )"
            for feature in feature_columns:
                create_table = create_table.replace(")", f", {feature} text)")
            for interface in interfaces:
                create_table = create_table.replace(")", f", {interface} integer)")
        
        if self.tablespace: create_table = create_table + f" tablespace {self.tablespace}"
    
        return create_table

    def get_copy_table_by_csv(self, csv_file_path, columns):
        str_col = ",".join(columns)
        sql = f'''
            COPY {self.schema}.{self.tablename} ({str_col}) FROM '{csv_file_path}' DELIMITER ';' CSV HEADER;
        '''
        return sql

    def generate_csv_file_path(self, csv_path, dataframe):
        csv_file_path = f"{csv_path}{self.tablename}.csv"
        dataframe.to_csv(csv_file_path, index=False, header=True, sep =';')
        return csv_file_path

    def read_table(self):
        return pd.read_sql_query(f'select * from {self.schema}.{self.tablename}',con=self.engine)

    def execute_copy(self, sql):
        print(f"Open Connection!")
        pg_conn = psycopg2.connect(self.urlconnect)
        cur = pg_conn.cursor()
        print(f"Start SQL execution: [{sql}]")
        cur.execute(sql)
        print(f"Finish SQL!")
        pg_conn.commit()
        cur.close()
        pg_conn.close()

    def execute_sql(self, sql):
        print(f"Open Connection!")
        conn = self.engine.connect()
        print(f"Start SQL execution: [{sql}]")
        conn.execute(sql)
        print(f"Finish SQL!")
        conn.close()
        print(f"Close Connection!")
        
        