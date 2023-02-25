import os, json

from collections import namedtuple

class Config:
    def _convert_json_into_object(self, data):
        object_hook = lambda d: namedtuple('x', d.keys())(*d.values())
        return json.load(data, object_hook=object_hook)

    @staticmethod
    def read_config_file_with_name(self, file_name):
        base_path = os.path.abspath('.') 
        with open(f'{base_path}/config/{file_name}.json', 'r') as json_file:
            object_hook = lambda d: namedtuple('x', d.keys())(*d.values())
            return json.load(json_file, object_hook=object_hook)

    @staticmethod
    def get_absolute_path(file_path):
        absolute_path = os.path.join(os.path.dirname( __file__ ))
        return os.path.join(absolute_path, file_path)