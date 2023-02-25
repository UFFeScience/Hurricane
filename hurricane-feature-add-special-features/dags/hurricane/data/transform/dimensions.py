from math import cos, asin, sqrt
from shapely.geometry import Point, Polygon, LineString
import networkx as nx
import osmnx as ox
import pandas as pd
import numpy as np

class Dimensions:
    def __init__(self, datalake, workdir):
        self.datalake = datalake
        self.workdir  = workdir

    def get_nearest_segment_by_point(self, segments, find_point):
        index_nearest_segment = 0
        distance_nearest_segment = 99999999
        point = np.array([find_point['lat'], find_point['lon']])
        for index, s in segments.iterrows():
            lon_star, lat_star = s['vertice_star'].split(",")
            lon_final, lat_final = s['vertice_final'].split(",")
            # Check Warning cases -> where the linestring is a circle, that is, the start and end points are equal 
            if lon_star == lon_final and lat_star == lat_final:
                lon_final = float(lon_final) - 0.000001
            p1 = np.array([float(lat_star), float(lon_star)])
            p2 = np.array([float(lat_final), float(lon_final)])            
            distance_segment = np.linalg.norm(np.cross(p2 - p1, p1 - point))/np.linalg.norm(p2 - p1)            
            if distance_segment < distance_nearest_segment:
                distance_nearest_segment = distance_segment
                index_nearest_segment = index

        return segments.loc[index_nearest_segment]

    def get_distance(self, lat1, lon1, lat2, lon2):
        p   = 0.017453292519943295
        hav = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
        return 12742 * asin(sqrt(hav))

    def get_closest_point(self, coordinates, xy):
        # coordinates = {'id': ?, 'lat': ?, 'lon': ?}
        return min(coordinates, key = lambda point: 
            self.get_distance(xy['lat'], xy['lon'], point['lat'], point['lon'])
        )
    
    def create_segments(self, gdf_edges):
        precision = 7
        segments = []
        for index, line in gdf_edges.iterrows():
            vertices = ox.utils_geo.redistribute_vertices(line['geometry'], 0.0009)
            for i in range(len(vertices) - 1):
                current_item = ox.utils_geo.round_geometry_coords(vertices[i], precision)
                next_item = ox.utils_geo.round_geometry_coords(vertices[i + 1], precision)
                distance = self.get_distance(current_item.x, current_item.y, next_item.x, next_item.y) * 1000 # transform in meters
                segments.append([
                    line['oneway'],
                    line['name'] if type(line['name']) == list else [line['name']],
                    line['highway'] if type(line['highway']) == list else [line['highway']],
                    current_item, # start_ver
                    next_item,    # final_ver
                    round(distance, 2)])
        df_segments = pd.DataFrame(segments,columns=['oneway', 'name', 'highway', 'star_ver', 'final_ver', 'length'])
        df_segments['name'] = df_segments['name'].apply(lambda s: [] if s == [np.nan] else s)
        df_segments = df_segments.loc[df_segments.astype(str).drop_duplicates().index]

        # Generate Sequence Id
        df_segments.reset_index(inplace=True, drop=True)
        df_id = pd.DataFrame([number for number in range(1, len(df_segments.index))],columns=['seg_id'])
        df_full_segments = pd.DataFrame()
        df_full_segments = pd.concat([df_id, df_segments], axis=1, join='inner')
        
        # Transform Point to Pair
        df_full_segments['star_ver'] = df_full_segments['star_ver'].apply(lambda row :  str(row.x) + "," + str(row.y))
        df_full_segments['final_ver'] = df_full_segments['final_ver'].apply(lambda row : str(row.x) + "," + str(row.y))

        return df_full_segments

    def create_vertices(self, df_segments):
        # Create Vertices
        star_ver  = df_segments['star_ver'].copy()
        final_ver = df_segments['final_ver'].copy()
        df_vertex = star_ver.append(final_ver, ignore_index=True)
        df_vertex = pd.DataFrame(np.unique(df_vertex), columns={'vertice'})
        df_vertex.reset_index(inplace=True, drop=True)

        df_id = pd.DataFrame([number for number in range(1, len(df_vertex.index))],columns=['ver_id'])
        df_full_vertex = pd.DataFrame()
        df_full_vertex = pd.concat([df_id, df_vertex], axis=1, join='inner')

        return df_full_vertex
    
    def associate_zones(self, df_vertex, df_zones):
        df_vertex['zone_id'] = 0
        for index_point, point in df_vertex.iterrows():
            for _, zone in df_zones.iterrows():
                x, y = point['vertice'].split(",", maxsplit=2)
                if zone['geometry'].contains(Point(float(x), float(y))):
                    df_vertex.loc[index_point, 'zone_id'] = zone['id']
                    break
        return df_vertex

    def associate_districts(self, df_vertex, df_districts):
        df_vertex['district_id'] = 0
        for vid, point in df_vertex.iterrows():
            x, y = point['vertice'].split(",", maxsplit=2)
            p = Point(float(x), float(y))
            for did, district in df_districts.iterrows():
                if district['geometry'].contains(p):
                    df_vertex.loc[vid, 'district_id'] = district['id']
                    break
        return df_vertex

    def associate_neighborhoods(self, df_vertex, df_neighborhoods):
        df_vertex['neighborhood_id'] = 0
        for vid, point in df_vertex.iterrows():
            x, y = point['vertice'].split(",", maxsplit=2)
            p = Point(float(x), float(y))
            for nid, neighborhood in df_neighborhoods.iterrows():
                if neighborhood['geometry'].contains(p):
                    df_vertex.loc[vid, 'neighborhood_id'] = neighborhood['id']
                    break
        return df_vertex

    def merge_vertices_into_segments(self, df_vertices, df_segments):
        df_final_segments = df_segments.merge(df_vertices, left_on=['star_ver'], right_on=['vertice'],  how='inner')
        df_final_segments.rename(columns={"ver_id" : "ver_id_star"}, inplace=True)
        df_final_segments.drop(columns=['star_ver', 'vertice', 'zone_id', 'district_id', 'neighborhood_id'], inplace=True, errors='ignore')

        df_final_segments = df_final_segments.merge(df_vertices, left_on=['final_ver'], right_on=['vertice'],  how='inner')
        df_final_segments.rename(columns={"ver_id" : "ver_id_final"}, inplace=True)
        df_final_segments.drop(columns=['final_ver', 'vertice', 'zone_id', 'district_id', 'neighborhood_id'], inplace=True, errors='ignore')
        
        return df_final_segments
    
    def transform_vertices_in_list(self, df):
        vertices = []
        for _, vertice in df.iterrows():
            y, x = vertice['vertice'].split(",")
            template = {
                'ver_id' : vertice['ver_id'],
                'lat'   : float(x),
                'lon'   : float(y)
            }
            vertices.append(template)
        return vertices

    def filter_uniqueway_segments(self, df_segments):
        oneway = df_segments[df_segments['oneway'] == True].copy()
        twoway = df_segments[df_segments['oneway'] == False].copy()
        waylist = []
        for index, way in twoway.iterrows():
            if way['ver_id_star'] > way['ver_id_final']:
                star = way['ver_id_final']
                final = way['ver_id_star']
            else:
                star = way['ver_id_star']
                final = way['ver_id_final']

            row = {
                'seg_id' : way['seg_id'],
                'oneway' : way['oneway'],
                'name'   : way['name'],
                'highway': way['highway'],
                'length' : way['length'],
                'ver_id_star' : star,
                'ver_id_final' : final
            }
            waylist.append(row)
        df_twoway_segments = way = pd.DataFrame.from_dict(waylist)
        df_twoway_segments.sort_values(by=['ver_id_star', 'ver_id_final', 'seg_id'], inplace=True)
        df_twoway_segments.drop_duplicates(subset=['ver_id_star', 'ver_id_final'], keep='first', inplace=True)
        df_full_segments = pd.concat([oneway, df_twoway_segments], ignore_index=True)
        # Circular streets, where the start and end points are less than 100 meters
        df_full_segments = df_full_segments[~((df_full_segments['length'] == 0) | (df_full_segments['ver_id_star'] == df_full_segments['ver_id_final']))]
        return df_full_segments
