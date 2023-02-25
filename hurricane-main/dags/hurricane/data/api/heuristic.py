import pandas as pd
import numpy as np
import os
from shapely.geometry import Point
from hurricane.data.api.box import Box
from hurricane.data.api.config import Config
from hurricane.data.extract.dimensions import Dimensions
from hurricane.data.extract.infos import Infos
from hurricane.utils.datalake import Datalake
from sqlalchemy import create_engine

class Heuristic:
    def __init__(self, datalake, dag_id):
        self.dag_id = dag_id
        self.model_config, self.info_config = Config(datalake, self.dag_id).get_configs()
        if self.model_config is not None:
            self.engine     = create_engine(self.model_config['postgresql_url'])
            self.workdir    = self.model_config['datalake_workdir']
            self.datalake   = Datalake(self.model_config['datalake_client'], self.workdir)
            self.dimensions = Dimensions(self.datalake, self.workdir, self.engine)
            self.segments   = self.dimensions.get_segments_from_silver()
            self.vertices   = self.dimensions.get_vertices_from_silver()
            self.zones      = self.dimensions.get_zones_from_database()
            if self.info_config is not None:
                self.infos      = Infos(self.datalake, self.info_config['datalake_workdir'], self.info_config)
                self.fact       = self.infos.get_infos_from_gold()
                if self.fact is None or self.fact.empty:
                    raise Exception(f"Fact Table {self.info_config['datalake_workdir']} not found!!!")
            else:
                raise Exception(f'Info Configuration file for Workdir = {self.workdir} not found!!!')
        else:
            raise Exception(f'Model Configuration file for DagId = {self.dag_id} not found!!!')

    def get_infos(  self,
                    zones,
                    number_police,
                    number_inter_zone_routes,
                    number_intra_zone_routes,
                    max_distance_inter_zone,
                    max_distance_intra_zone,
                    vip_routes,
                    filename,
                    distance_in_km=0.0,
                    isCircle=False):
        coords = []
        object_box = None

        # get selected zones
        selected_zones = self.zones[self.zones['id'].isin(zones)]
        selected_zones = self.dimensions.filter_default_value(selected_zones)
        for _, z in selected_zones.iterrows():
            for coord in z['geometry'].exterior.coords:
                coords.append([coord[1], coord[0]])
    
        # create object_box from segments
        box = Box(coords)
        if(isCircle): object_box = box.create_cbox(distance_in_km)
        else:         object_box = box.create_bbox(distance_in_km)

        # get all vertices in bounding box
        l_vertices = []
        for i, v in self.vertices.iterrows():
            y, x  = v['vertice'].split(",", maxsplit=2)
            p = Point(float(x), float(y))
            if object_box.contains(p): l_vertices.append(v['ver_id'])
        
        segments_by_start = self.segments[self.segments['ver_id_star'].isin(l_vertices)]
        segments_by_final = self.segments[self.segments['ver_id_final'].isin(l_vertices)]
        segments_group    = pd.concat([segments_by_start, segments_by_final])
        segments_group    = segments_group.drop_duplicates(subset=['seg_id', 'ver_id_star', 'ver_id_final'], ignore_index=True)

        # Circular streets, where the start and end points are less than 100 meters
        segments_group    = segments_group[~((segments_group['length'] == 0) | (segments_group['ver_id_star'] == segments_group['ver_id_final']))]

        vertices_group    = self.vertices[
            (self.vertices['ver_id'].isin(segments_group['ver_id_star'].unique()))
          | (self.vertices['ver_id'].isin(segments_group['ver_id_final'].unique()))]
        vertices_group    = vertices_group.drop_duplicates(subset=['ver_id'], ignore_index=True)

        infos_group      = self.fact[self.fact['seg_id'].isin(segments_group['seg_id'].unique())].copy()

        info_columns = []
        for info in infos_group.columns:
            if info not in ['seg_id', 'time_id']: info_columns.append(info)

        infos_group['total'] = infos_group[info_columns].sum(axis=1)
        
        drop_columns = info_columns
        drop_columns.append('time_id')
        infos_group.drop(columns=drop_columns, inplace=True)
        
        infos_group = infos_group.groupby(['seg_id']).agg({"total": "sum"}).reset_index()
        result_group = segments_group.merge(infos_group, on=['seg_id'], how='left')

        # fill empty values
        result_group['total']        = result_group['total'].fillna(value=0)
        result_group['length']       = result_group['length'].round(2)
        result_group['ver_id_star']  = result_group['ver_id_star'].astype(int)
        result_group['ver_id_final'] = result_group['ver_id_final'].astype(int)
        result_group['total']        = result_group['total'].astype(int)
        
        result_group = result_group.drop_duplicates(subset=['ver_id_star','ver_id_final'])
        
        equalize_twoway_factor = result_group[result_group['total'] > 0]
        for i, r in equalize_twoway_factor.iterrows():
            find_r = result_group[
                (result_group['ver_id_star'] == r['ver_id_final']) &
                (result_group['ver_id_final'] == r['ver_id_star']) &
                (result_group['total'] == 0)
            ]
            if not find_r.empty:
                result_group.loc[find_r.index,'total'] = r['total']

        output_dir = os.path.dirname(filename)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        f = open(filename, "w")
        
        # Info -> Routes
        string = "{n_vertices} {n_segments} {n_zones} {n_polices} {n_inter_zone_routes} {n_intra_zone_routes} {distance_inter_zone} {distance_intra_zone}\n"
        f.write(
            string.format(
                  n_vertices = len(vertices_group.index)
                , n_segments = len(result_group.index)
                , n_zones = len(vertices_group['zone_id'].unique())
                , n_polices = number_police
                , n_inter_zone_routes = number_inter_zone_routes
                , n_intra_zone_routes = number_intra_zone_routes
                , distance_inter_zone = max_distance_inter_zone
                , distance_intra_zone = max_distance_intra_zone
            )
        )

        # Info -> Vertices
        for _, v in vertices_group.iterrows():
            vertice = "{id} {zone}\n"
            f.write(
                vertice.format(
                    id = v['ver_id']
                  , zone = v['zone_id']
                )
            )
        
        # Info -> Segments and Crimes
        for _, r in result_group.iterrows():
            segment = "{id_start} {id_final} {total} {length}\n"
            f.write(
                segment.format(
                    id_start = r['ver_id_star']
                  , id_final = r['ver_id_final']
                  , total = r['total']
                  , length = r['length']
                )
            )

        # Info -> Vip Routes
        for v in vip_routes:
            vip = "{vip_route}\n"
            f.write(vip.format(vip_route=v))

        f.close()
