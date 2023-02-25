from hurricane.utils.datalake import Datalake
from hurricane.data.extract.dimensions import Dimensions as ExtractDimensions
from hurricane.data.transform.dimensions import Dimensions as TransformDimensions
from hurricane.data.transform.domains import Domains as TransformDomains
from hurricane.data.extract.infos import Infos as ExtractInfos

import pandas as pd
import numpy as np

class Infos:
    def process_check_model_and_info(**args):
        dag_id          = args['dag_id']
        adl             = args['adl']
        workdir         = args['workdir']
        partitions      = args['number_partitions']
        config          = args['config_json']
        related_config  = args['related_config_json']
        datalake        = Datalake(adl, dag_id)
        info            = ExtractInfos(datalake, workdir, config)
        extract         = ExtractDimensions(datalake, related_config['datalake_workdir'])
        model_changed   = False

        df_model_historic_dates = extract.get_historic_date_from_silver()
        df_info_historic_dates = info.get_historic_dates_from_silver()
        if df_info_historic_dates.empty:
            model_changed = True
            print("First run after model creation!")
            datalake.write_parquet_file_from_dataframe(info.silver_historic_dates, df_model_historic_dates)
        else:
            if df_info_historic_dates['historic_date'].max() == df_model_historic_dates['historic_date'].max():
                print("The latest version of the template hasn't changed yet!")
            else:
                print("The latest version of the template has been changed!")
                model_changed = True
                for p in range(1, partitions):
                    filename = info.get_infos_filename_partition(p)
                    if datalake.verify_file_exists(filename):
                        print(f'Backup file, move [{filename}] to [{info.silver_path + "historic/"}]')
                        datalake.move_file_to_historic(filename, info.silver_path + "historic/", suffix = "", append_date=True)
                if datalake.verify_file_exists(info.gold_info_file_path):
                    print(f'Backup file, move [{info.gold_info_file_path}] to [{info.gold_path + "historic/"}]')
                    datalake.move_file_to_historic(info.gold_info_file_path, info.gold_path + "historic/", suffix = "", append_date=True)
                datalake.write_parquet_file_from_dataframe(info.silver_historic_dates, df_model_historic_dates)
        
        args['ti'].xcom_push(key="model_changed", value=model_changed)

    def process_silver_infos(**args):
        dag_id          = args['dag_id']
        adl             = args['adl']
        workdir         = args['workdir']
        partition       = args['partition']
        config          = args['config_json']
        related_config  = args['related_config_json']
        datalake        = Datalake(adl, dag_id)
        info            = ExtractInfos(datalake, workdir, config)
        extract         = ExtractDimensions(datalake, related_config['datalake_workdir'])
        transform       = TransformDimensions(datalake, related_config['datalake_workdir'])

        df_bronze_partition = info.get_infos_partition_from_bronze(partition)
        silver_columns_compare = info.silver_columns_compare + info.feature_columns
        
        if not df_bronze_partition.empty:
            df_silver_partition = info.get_infos_partition_from_silver(partition)
            df_silver_segments  = transform.filter_uniqueway_segments(extract.get_segments_from_silver())
            df_silver_vertices  = extract.get_vertices_from_silver()
            vertices_list       = transform.transform_vertices_in_list(df_silver_vertices)

            df_full_segments    = df_silver_segments.merge(df_silver_vertices, left_on=['ver_id_star'], right_on=['ver_id'])
            df_full_segments    = df_full_segments.drop(['ver_id', 'zone_id'], axis=1)
            df_full_segments.rename(columns = {"vertice": "vertice_star"}, inplace=True)

            df_full_segments    = df_full_segments.merge(df_silver_vertices, left_on=['ver_id_final'], right_on=['ver_id'])
            df_full_segments    = df_full_segments.drop(['ver_id', 'zone_id'], axis=1)
            df_full_segments.rename(columns = {'vertice': 'vertice_final'}, inplace = True)
                    
            df_bronze_partition = df_bronze_partition.sort_values(['LATITUDE','LONGITUDE'])
            df_bronze_partition = df_bronze_partition[silver_columns_compare]
            df_bronze_partition.dropna(subset=['LATITUDE','LONGITUDE', 'DATE'], inplace=True)

            if not df_silver_partition.empty:
                silver_compare = df_silver_partition[silver_columns_compare].copy()
                diff = df_bronze_partition.merge(silver_compare, how = 'outer', indicator=True).loc[lambda x : x['_merge']=='left_only']
                diff.reset_index(drop=True, inplace=True)
            else:
                diff = df_bronze_partition.copy()

            grouped_infos_by_latlon = diff.groupby(['LATITUDE','LONGITUDE'])

            for _, group in grouped_infos_by_latlon:
                # get the closest vertice id
                find = {'lat': float(group['LATITUDE'].iloc[0]), 'lon': float(group['LONGITUDE'].iloc[0])}
                closest_vertice = transform.get_closest_point(vertices_list, find)
                # based on the vertice id found get the segment id 
                filter = (df_full_segments['ver_id_star'] == closest_vertice['ver_id']) | (df_full_segments['ver_id_final'] == closest_vertice['ver_id'])
                closest_segments = df_full_segments[filter].reset_index(drop=True)
                if len(closest_segments.index) > 1:
                    closest_vertice = transform.get_nearest_segment_by_point(closest_segments, find)
                    diff.loc[group.index, 'seg_id'] = closest_vertice['seg_id']
                else:
                    diff.loc[group.index, 'seg_id'] = closest_segments['seg_id'].iloc[0]

            df_infos = pd.concat([df_silver_partition, diff], ignore_index=True)
            df_infos = df_infos.drop(['_merge'], errors='ignore', axis=1)
            df_infos.reset_index(drop=True, inplace=True)

            datalake.write_parquet_file_from_dataframe(info.get_infos_filename_partition(partition), df_infos)

    def process_infos(**args):
        number_partitions = args['number_partitions']
        
        dag_id    = args['dag_id']
        adl       = args['adl']
        workdir   = args['workdir']
        config    = args['config_json']
        datalake  = Datalake(adl, dag_id)
        info      = ExtractInfos(datalake, workdir, config)
        domain    = TransformDomains(datalake, workdir)

        df_silver_infos = info.get_all_infos_from_silver(number_partitions)
        df_silver_times = domain.get_times_from_silver()
        
        fixed_columns = ['time_id', 'seg_id', 'INFO']
        gold_columns  = fixed_columns + info.feature_columns
        index_columns = gold_columns.copy()
        index_columns.remove('INFO')

        for column in info.feature_columns:
            df_silver_infos[column] = df_silver_infos[column].apply(lambda value : str(value).replace("nan", ""))
        
        df_silver_infos['period']  = df_silver_infos['PERIOD']
        df_silver_infos['weekday'] = df_silver_infos.apply( lambda row: domain.days_of_week[row['DATE'].weekday()], axis = 1)
        df_silver_infos['day']     = df_silver_infos.apply( lambda row: row['DATE'].day, axis = 1)
        df_silver_infos['month']   = df_silver_infos.apply( lambda row: row['DATE'].month, axis = 1)
        df_silver_infos['year']    = df_silver_infos.apply( lambda row: row['DATE'].year, axis = 1)

        df_infos = df_silver_infos.merge(df_silver_times, on=['period', 'weekday', 'day', 'month', 'year'])
        df_infos = df_infos.drop(['period', 'weekday', 'day', 'month', 'year'], axis=1)

        df_infos = df_infos.groupby(gold_columns)['INFO'].count().reset_index(name="count")
        df_infos = pd.pivot_table(df_infos, values='count', index=index_columns, columns=['INFO'], fill_value=0).reset_index()
        
        datalake.write_parquet_file_from_dataframe(info.gold_info_file_path, df_infos)

