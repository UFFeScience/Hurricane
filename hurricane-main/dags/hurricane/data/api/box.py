from geojson import Feature
from shapely.geometry import Polygon, LineString
from turfpy.transformation import circle
from hurricane.data.transform.domains import Domains
import numpy  as np

class Box:
    # 2D bounding box
    def __init__(self, points):
        self.km = 0.0089
        self.unit_degree = 1.11
        if len(points) == 0:
            raise ValueError("Can't compute bounding box of empty list")
        self.min_x, self.min_y = float("inf"), float("inf")
        self.max_x, self.max_y = float("-inf"), float("-inf")
        for x, y in points:
            # Set min coords
            if x < self.min_x: self.min_x = x
            if y < self.min_y: self.min_y = y
            # Set max coords
            if x > self.max_x:   self.max_x = x
            elif y > self.max_y: self.max_y = y
    
    @property
    def width(self):
        return self.max_x - self.min_x
    
    @property
    def height(self):
        return self.max_y - self.min_y
    
    def __repr__(self):
        return "BoundingBox({}, {}, {}, {})".format(self.min_x, self.max_x, self.min_y, self.max_y)
    
    def create_bbox(self, distance_in_km=0.0):
        bbox = np.array([
            [self.min_x - distance_in_km * self.km, self.min_y - distance_in_km * self.km],
            [self.min_x - distance_in_km * self.km, self.max_y + distance_in_km * self.km],
            [self.max_x + distance_in_km * self.km, self.max_y + distance_in_km * self.km],
            [self.max_x + distance_in_km * self.km, self.min_y - distance_in_km * self.km],
            [self.min_x - distance_in_km * self.km, self.min_y - distance_in_km * self.km]])
        return Polygon(LineString(bbox))

    def create_cbox(self, distance_in_km=0.0):
        distance = 0.0
        domain = Domains(None, None)
        bbox = self.create_bbox().exterior.coords
        centroid = bbox.centroid
        # Calculate distance [centroid-> max(box border)]
        for p in bbox:
            d = domain.get_distance(p[0],p[1], centroid.x, centroid.y)
            if d > distance: distance = d        
        cbox = circle(center=Feature(geometry=centroid), radius = (distance + distance_in_km) * self.unit_degree, steps=64)
        return Polygon(LineString(cbox.geometry.coordinates[0]))
