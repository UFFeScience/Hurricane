import networkx as nx
import osmnx as ox
import pandas as pd
import numpy as np
from datetime import datetime
from hurricane.utils.datalake import Datalake
from hurricane.data.extract.dimensions import Dimensions as ExtractDimensions
from hurricane.data.transform.dimensions import Dimensions as TransformDimensions

import pandas as pd

class Dimensions:
    def process_create_segments(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir)
        transform  = TransformDimensions(datalake, workdir)

        # backup and create new segments database 
        if datalake.verify_file_exists(extract.silver_vertice_file_path) and datalake.verify_file_exists(extract.silver_segment_file_path):
            # Map data for future loading
            current_date = datetime.now().date()
            df_current_date = pd.DataFrame.from_dict([{"historic_date" : current_date}])
            df_historic_dates = pd.concat([extract.get_historic_date_from_silver(), df_current_date], ignore_index=True).drop_duplicates()

            if datalake.verify_file_exists(extract.silver_historic_date_file_path):
                df_exists_dates = datalake.read_parquet_file(extract.silver_historic_date_file_path)
                if not df_exists_dates.empty:
                    if current_date in df_exists_dates['historic_date'].to_list():
                        raise Exception(f"It is not possible to recreate the model more than once a day [{current_date}]")
                    else:
                        print(f"Creating a new instance of the model for the day[{current_date}]")

            vertice_historic_path = extract.silver_vertice_file_path.replace("vertice.parquet", f"historic/")
            segment_historic_path = extract.silver_segment_file_path.replace("segment.parquet", f"historic/")
            
            print(f"Backup file, move [{extract.silver_vertice_file_path}] to [{vertice_historic_path}]")
            datalake.move_file_to_historic(extract.silver_vertice_file_path, vertice_historic_path, suffix = "", append_date=True)

            print(f"Backup file, move [{extract.silver_segment_file_path}] to [{segment_historic_path}]")
            datalake.move_file_to_historic(extract.silver_segment_file_path, segment_historic_path, suffix = "", append_date=True)
            datalake.write_parquet_file_from_dataframe(extract.silver_historic_date_file_path, df_historic_dates)

        places = []
        if args['places'] is None or args['places'] == "":
            raise Exception("Please inform the search location!")
        for place in args['places']: 
            if place['city'] is None or place['city'] == "":
                raise Exception("Please inform the search city!")
            elif place['state'] is None or place['state'] == "":
                raise Exception("Please inform the search state!")
            elif place['country'] is None or place['country'] == "":
                raise Exception("Please inform the search country!")
            else:
                places.append(place['city'] + ", " + place['state'] + ", " + place['country'])

        # download/model a street network for some city then visualize it
        ox.config(
            log_console=True,
            cache_folder=f'{workdir}/cache',
            use_cache=True
        )
        G = ox.graph_from_place(places, network_type="drive")
        _, gdf_edges = ox.graph_to_gdfs(G)

        df_segments = transform.create_segments(gdf_edges)

        datalake.write_parquet_file_from_dataframe(extract.bronze_segments_file_path, df_segments)

    def process_create_vertices(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir)
        transform  = TransformDimensions(datalake, workdir)
        
        df_bronze_segments = datalake.read_parquet_file(extract.bronze_segments_file_path)
        df_vertices = transform.create_vertices(df_bronze_segments)

        datalake.write_parquet_file_from_dataframe(extract.bronze_vertices_file_path, df_vertices)

    def process_associate_zones(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        engine     = args['engine']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir, engine)
        transform  = TransformDimensions(datalake, workdir)
        
        df_zones = extract.filter_default_value(extract.get_zones_from_database())
        df_bronze_vertices = datalake.read_parquet_file(extract.bronze_vertices_file_path)

        df_vertices = transform.associate_zones(df_bronze_vertices, df_zones)

        datalake.write_parquet_file_from_dataframe(extract.bronze_vertices_file_path, df_vertices)

    def process_associate_districts(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        engine     = args['engine']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir, engine)
        transform  = TransformDimensions(datalake, workdir)

        df_bronze_vertices = datalake.read_parquet_file(extract.bronze_vertices_file_path)
        df_districts       = extract.filter_default_value(extract.get_districts_from_database())
        df_vertices        = transform.associate_districts(df_bronze_vertices, df_districts)

        datalake.write_parquet_file_from_dataframe(extract.bronze_vertices_file_path, df_vertices)

    def process_associate_neighborhoods(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        engine     = args['engine']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir, engine)
        transform  = TransformDimensions(datalake, workdir)

        df_bronze_vertices = datalake.read_parquet_file(extract.bronze_vertices_file_path)
        df_neighborhoods   = extract.filter_default_value(extract.get_neighborhoods_from_database())
        df_vertices        = transform.associate_neighborhoods(df_bronze_vertices, df_neighborhoods)

        datalake.write_parquet_file_from_dataframe(extract.bronze_vertices_file_path, df_vertices)

    def process_merge_vertices_into_segments(**args):
        dag_id     = args['dag_id']
        adl        = args['adl']
        workdir    = args['workdir']
        datalake   = Datalake(adl, dag_id)
        extract    = ExtractDimensions(datalake, workdir)
        transform  = TransformDimensions(datalake, workdir)

        df_bronze_vertices = datalake.read_parquet_file(extract.bronze_vertices_file_path)
        df_bronze_segments = datalake.read_parquet_file(extract.bronze_segments_file_path)
        df_silver_segments = transform.merge_vertices_into_segments(df_bronze_vertices, df_bronze_segments)

        datalake.write_parquet_file_from_dataframe(extract.silver_vertice_file_path, df_bronze_vertices)
        datalake.write_parquet_file_from_dataframe(extract.silver_segment_file_path, df_silver_segments)

