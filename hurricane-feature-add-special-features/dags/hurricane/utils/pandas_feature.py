import pandas as pd
from unidecode import unidecode
import text as text_util


class Pandas:
    @staticmethod
    def clean(df):
        df.fillna('', inplace=True)
        df.columns = df.columns.str.strip()
        return df 

    @staticmethod
    def all_string(df):
        df[list(df)] = df[list(df)].astype(str)
        return df 

    @staticmethod
    def trim_columns(df):
        df.columns = df.columns.str.strip()
        return df 

    @staticmethod
    def upper_columns(df):
        df.columns = df.columns.str.upper()
        return df 

    @staticmethod
    def normalize_header(df, only_alphanumeric=True):
        headers = df.columns

        #Strip, only ASCI characters and upper
        headers = [unidecode(header).strip()
                                    .replace(' ?', '')
                                    .replace(' !', '')
                                    .replace('_?', '')
                                    .replace('_!', '')
                                    .replace('?', '')
                                    .replace('!', '')
                                    .replace(' _ ', '_')    
                                    .replace(' - ', '_')
                                    .replace(' / ', '_')
                                    .replace(' ', '_')
                                    .replace('-', '_')
                                    .replace('/', '_')                                    
                                    .upper() for header in headers]

        if only_alphanumeric:
            headers = [text_util.extract_alphanumeric(header) for header in headers]

        df.columns = headers
        return df

    @staticmethod
    def strip_upper_values(df):
        trim_strings = lambda x: x.strip().upper() if isinstance(x, str) else x
        return df.applymap(trim_strings)

    @staticmethod
    def normalize_string_values(df, upper=False):
        normalize_lambda = lambda x: x if not isinstance(x, str) \
                                        else text_util.remove_accents(x.strip().upper() if upper else x.strip())
        return df.applymap(normalize_lambda)