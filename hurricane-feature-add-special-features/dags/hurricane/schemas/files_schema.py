import os
import json
import pandas as pd
import numpy as np
class FilesSchema:
    def __init__(self, dag_id):
        self.dag_id = dag_id

    def __validate_columns(self, required_columns, columns):
        print(f"Required Columns [{required_columns}]")
        print(f"Columns found in this file [{columns}]")
        diff = frozenset(required_columns).difference(columns)
        is_valid = True if len(diff) == 0 else False
        return is_valid, list(diff)

    def get_required_columns(self, schema):
        if schema is None:
            return None
        required_columns = [column['name'] for column in schema['columns'] if column['required']]
        if not required_columns:
            return None
        return required_columns

    def get_unrequired_columns(self, schema):
        if schema is None:
            return None
        required_columns = [column['name'] for column in schema['columns'] if not column['required']]
        if not required_columns:
            return None
        return required_columns

    def validate_file_schema(self, schema, columns):        
        required_columns = self.get_required_columns(schema)
        return self.__validate_columns(required_columns, columns)

    def get_required_columns_type(self, schema):
        if schema is None:
            return None
        required_columns = {column['name'] : column['type'] for column in schema['columns'] if column['required']}
        if not required_columns:
            return None
        return required_columns

    def get_columns_type(self, schema):
        if schema is None:
            return None
        type_columns = {column['name'] : column['type'] for column in schema['columns']}
        if not type_columns:
            return None
        return type_columns

    def convert_columns_by_type(self, df, schema):
        if df is None or df.empty:
            return df
        column_types = self.get_columns_type(schema)
        for column in df.columns:
            ctype = column_types[column].upper()
            if ctype in ['STR', 'STRING']:
                df[column] = df[column].apply(str)
                df[column].replace(np.nan, '', regex=True)
            elif ctype in ['INT', 'INTEGER']:
                df[column] = pd.to_numeric(df[column], errors='coerce').astype(int)
            elif ctype in ['FLOAT', 'DECIMAL']:
                df[column] = df[column].apply(lambda row: str(row).replace(',','.'))
                df[column] = pd.to_numeric(df[column], errors='coerce').astype(float)
            elif ctype in ['DATE', 'DATETIME']:
                df[column] = pd.to_datetime(df[column], errors="coerce")
            else:
                raise Exception(f'Column [{column}] conversion failed to [{ctype}]')       
        return df
