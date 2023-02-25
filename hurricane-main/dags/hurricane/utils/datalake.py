import chardet
import io
import openpyxl
import os
import pathlib
import pandas as pd

from datetime import datetime, date, timedelta
from pyxlsb import open_workbook as open_xlsb
from hurricane.schemas.files_schema import FilesSchema
from hurricane.utils.client import Client

class Datalake:
    def __init__(self, config_dir, dag_id):
        self.config_dir   = config_dir
        self.dag_id       = dag_id
        self.client       = Client()
        self.files_schema = FilesSchema(self.dag_id)

    @staticmethod
    def _file_path_validate(file_path, ext):
        file_extension = pathlib.Path(file_path).suffix

        if isinstance(ext, list):
            return file_extension in ext

        return ext == file_extension

    def _create_path_if_not_exists(self, path):
        """
        If the path does not exist, create it.
        Parameters
        ----------
        path: string
            - Path of data lake directory.
        """
        file_name = path.split('/')[-1]
        path = path.replace(file_name, '')
        try:
            if not self.client.exists(path):
                self.client.mkdir(path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Failed to generate directory [{e}]')
    
    def _verify_if_path_if_exists(self, path):
        """
        If the path exists, return True.
        Parameters
        ----------
        path: string
            - Path of data lake directory.
        """
        if self.client.exists(path):
            return True
        else:
            return False

    def move_file_to_historic (self, file, historic_path, suffix=".bkp", append_date=True):
        """
        Move file from a path to another historic path and overwrite if exists.
        Parameters
        ----------
        file: string
            - filename Path
        historic_path: string
            - Path of historic file
        Optional Parameters
        ----------
        suffix: string
            - suffix filename
        append_date: Boolean
            -  Include current date in the file name
        """
        date = datetime.now().strftime("%Y%m%d")
        file_name = pathlib.Path(file).with_suffix("").name
        file_ext = pathlib.Path(file).suffix
        historic = f'{historic_path}/{file_name}{"_" + date if append_date else ""}{file_ext}{suffix}'
        self.move_and_overwrite_file_from_to(file, historic)

    def move_and_overwrite_file_from_to(self, from_path, to_path):
        """
        Move file from a path to another path and overwrite if exists.
        Parameters
        ----------
        from_path: string
            - Path of file
        
        to_path: string
            - Path to move
        """
        self._create_path_if_not_exists(to_path)
        try:
            if self.client.exists(to_path):
                self.client.remove(to_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Failed to remove file [{e}]')

        try:
            self.client.mv(from_path, to_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Failed to move file [{e}]')

    def get_file_modification_date(self, file_path):
        """
        Get the date of the last edit of file.
        Parameters
        ----------
        file_path: string
            - Path of file.
        
        Return
        ------
        datetime
            - Date of last edit.
        """
        try:
            with self.client.open(file_path) as file:
                modificationTime = file.info()['modificationTime']
            return datetime.fromtimestamp(modificationTime/1000.0)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Failed to get file modification date [{e}]')

    def write_parquet_file_from_dataframe(self, file_path, dataframe):
        """
        Transform a pandas dataframe into a parquet file and save in data lake.
        Ps.: By default, this method tries to set the file columns data type using the values of the first row. 
             Otherwise, it will raise a conversion error.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self._create_path_if_not_exists(file_path)
        
        buffer = io.BytesIO()
        try:
            dataframe.to_parquet(buffer, index=False)
        except Exception:
            raise RuntimeError(f'Failed to generate parquet file.')
        try:
            with self.client.open(file_path, 'wb') as parquet_file:
                parquet_file.write(buffer.getbuffer())
        except Exception:
            raise RuntimeError(f'Failed to write file [{file_path}] in datalake')

    def write_csv_file_from_dataframe(self, file_path, dataframe):
        """
        Transform a pandas dataframe into a CSV file and save in data lake.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self._create_path_if_not_exists(file_path)
        try:
            content = dataframe.to_csv(index=False)
            content = bytes(content, 'utf-8')
        except Exception:
            raise RuntimeError(f'Failed to generate csv.')
        
        try:
            with self.client.open(file_path, 'wb') as file:
                file.write(content)
        except Exception:
            raise RuntimeError(f'Failed to generate file [{file_path}] in datalake')

    def write_xls_file_from_dataframe(self, file_path, dataframe, sheet_name="Sheet1", start_row=0):
        """
        Transform a pandas dataframe into a xls file and save in data lake.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self.__write_excel_file_from_dataframe(file_path, dataframe, sheet_name, 'xlwt', start_row)

    def write_xlsx_file_from_dataframe(self, file_path, dataframe, sheet_name="Sheet1", start_row=0):
        """
        Transform a pandas dataframe into an xlsx file and save in data lake.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self.__write_excel_file_from_dataframe(file_path, dataframe, sheet_name, "xlsxwriter", start_row)
    
    def __write_excel_file_from_dataframe(self, file_path, dataframe, sheet_name="Sheet1", engine='xlwt', start_row = 0):
        """
        Transform a pandas dataframe into an excel file and save in data lake.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self._create_path_if_not_exists(file_path)
        buffer = io.BytesIO()
        try:
            with pd.ExcelWriter(buffer, engine=engine) as writer:
                dataframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row,  engine=engine, index = False)
        except Exception:
            raise RuntimeError(f'Failed to generate excel [{file_path}].')
        try:
            with self.client.open(file_path, 'wb') as file:
                file.write(buffer.getbuffer())
        except Exception:
            raise RuntimeError(f'Failed to generate file [{file_path}] in datalake')

    def write_excel_file_multiple_sheets_from_dataframe(self, file_path, report_list, engine='xlsxwriter'):
        """
        Transform a pandas dataframe into an excel file and save in data lake.
        Parameters
        ----------
        file_path: string
            - Path to save the parquet file.
        dataframe: pandas.core.frame.DataFrame
            - Dataframe to transform.
        """
        self._create_path_if_not_exists(file_path)
        buffer = io.BytesIO()
        writer = pd.ExcelWriter(buffer, engine=engine)
        try:
            for sheet, df in report_list.items():
                df.to_excel(writer, sheet_name = sheet, index = False)
            writer.save()
        except Exception:
            raise RuntimeError(f'Failed to generate excel [{file_path}].')
        try:
            with self.client.open(file_path, 'wb') as file:
                file.write(buffer.getbuffer())
        except Exception:
            raise RuntimeError(f'Failed to generate file [{file_path}] in datalake')

    def read_parquet_file(self, file_path):
        """
        Transform a parquet file in data lake into a pandas dataframe.
        Parameter
        ---------
        file_path: string
            - Path to read the parquet file.
        
        Return
        ------
        file: pandas.core.frame.DataFrame
            - Dataframe of parquet file.
        """
        if not self._file_path_validate(file_path, '.parquet'):
            raise ValueError(f'This File [{file_path}] does not contain a correct extension')

        try:
            with self.client.open(file_path, 'rb') as parquet_file:
                file = pd.read_parquet(parquet_file)
                return file
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists.')

    def read_excel_file(self, file_path, header=0, usecols=None, sheet_name=0, dtype=None, engine='openpyxl'):
        """
        Transform an excel file in data lake into a pandas dataframe.
        Parameters
        ----------
        file_path: string
            - Path to read the parquet file.
        
        header: int
            - Index of the header row.
        
        use_cols: string
            - Interval of columns to be used. Ex: 'A:E'
        
        sheet_name: string
            - Name of the sheet to be extracted.
        Return
        ------
        file: pandas.core.frame.DataFrame
            - Dataframe of excel file.
        """
        ext = ['.xlsx', '.xls']
        if not self._file_path_validate(file_path, ext):
            raise ValueError(f'This file [{file_path}] does not contain a correct extension')

        try:
            with self.client.open(file_path, 'rb').encoding('latin1') as excel_file:
                file = pd.read_excel(excel_file, header=header, usecols=usecols, sheet_name=sheet_name, engine=engine)
            return file
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists')

    def read_file_content(self, file_path):
        """
        Extract lines of a file in data lake.
        Parameter
        ---------
        file_path: string
            - Path to read the parquet file.
        
        Return
        ------
        list
            - List with rows of file.
        """
        try:
            with self.client.open(file_path, 'rb') as file:
                rows = file.readlines()
            return [row.decode('utf-8') for row in rows]
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists.')

    def read_csv_file_on_dataframe(self, 
                                   file_path, 
                                   sep=';', 
                                   sep_decimal='.', 
                                   sep_thousands=None,  
                                   dtype=None, 
                                   parse_dates=False,
                                   encoding=None,
                                   engine=None,
                                   header=None):
        """
        Transform a CSV file in data lake into a pandas dataframe.
        Parameters
        ----------
        file_path: string
            - Path to read the CSV file.
        
        sep: string
            - CSV separator.
        
        sep_decimal: string
            - Decimal unit separator.
        
        sep_thousands: string
            - Thousand unit separator.
        
        dtype: list
            - List with data type of columns.
        
        parse_dates: list
            - List with columns to be parsed as a date.
        
        encoding: string
            - Type of encoding.
        Return
        ------
        file: pandas.core.frame.DataFrame
            - Dataframe of excel file.
        """
        if not self._file_path_validate(file_path, '.csv'):
            raise ValueError(f'This file {file_path} does not contain a correct extension')

        file_encoding = self.get_encoding(file_path) if encoding is None else encoding
        try:
            with self.client.open(file_path) as file:
                df = pd.read_csv(
                    file, 
                    sep=sep, 
                    decimal=sep_decimal, 
                    thousands=sep_thousands, 
                    encoding=file_encoding, 
                    dtype=dtype, 
                    parse_dates=parse_dates,
                    engine=engine,
                    header=header)
            return df
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists.')

    def read_xlsb_file_on_dataframe(self, 
                                    file_path, 
                                    sheet_name=0,
                                    header=0,
                                    use_cols=None,
                                    dtype=None,
                                    names=None):
        """
        Transform a xlsb file in data lake into a pandas dataframe.
        Parameters
        ----------
        file_path: string
            - Path to read the csv file.
        
        sheet_name: string
            - Name of the sheet to be extracted.
        
        header: int
            - Index of the header row.
        
        use_cols: string
            - Interval of columns to be used. Ex: 'A:E'
        
        dtype: dict
            - Typing dictionary for columns
        names: list
            - List of column names to use.
        
        Return
        ------
        df: pandas.core.frame.DataFrame
            - Dataframe of the xlsb file.
        """
        if not self._file_path_validate(file_path, '.xlsb'):
            raise ValueError(f'This file {file_path} does not contain a correct extension')

        try:
            with self.client.open(file_path) as file:
                df = pd.read_excel(
                    file,
                    engine ='pyxlsb',
                    sheet_name = sheet_name,
                    header = header,
                    usecols = use_cols,
                    dtype = dtype,
                    names=names)    
            return df
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists')

    def read_xlsx_file_on_dataframe(self, file_path, sheet_name=0, header=0, use_cols=None, dtype=None, engine='openpyxl'):
        """
        Transform an xlsx file in data lake into a pandas dataframe.
        Parameters
        ----------
        file_path: string
            - Path to read the csv file.
        
        header: int
            - Index of the header row.
        use_cols: string
            - Interval of columns to be used. Ex: 'A:E'
        
        dtype: dict
            - Typing dictionary for columns
            
        Return
        ------
        df: pandas.core.frame.DataFrame
            - Dataframe of the xlsx file.
        """
        if not self._file_path_validate(file_path, '.xlsx'):
            raise ValueError(f'O arquivo {file_path} does not contain a correct extension')

        try:
            with self.client.open(file_path) as file:
                df = pd.read_excel(file, sheet_name=sheet_name, header=header, usecols=use_cols, dtype=dtype, engine=engine)
            return df
        except FileNotFoundError as e:
            raise FileNotFoundError(f'File [{e}] does not exists.')

    def remove_file(self, file_path):
        """
        Remove file in data lake.
        Parameter
        ---------
        file_path: string
            - Data lake path of the file.
        """
        try:
            self.client.remove(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f'This file [{file_path}] does not exists.')

    def verify_file_exists(self, file_path):
        """
        Verify if the file exists in data lake.
        Parameter
        ---------
        file_path: string
            - Data lake path of the file.
        """
        try:
            return self.client.exists(file_path)
        except Exception:
            raise FileNotFoundError(f'Failed to check is file exists [{file_path}].')

    def list_dir(self, directory):
        """
        Return a list of file paths in the data lake directory.
        
        Parameter
        ---------
        directory: string
            - Data lake directory path.
        Return
        ------
        list
            - List with file paths in directory.
        """
        try:
            return self.client.ls(directory)
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Directory [{e}] does not exists')

    def write_file_content(self, file_path, rows):
        """
        Write file in data lake by file rows.
        Parameters
        ----------
        file_path: string
            - Path to save the file.
        
        rows: list
            - List with file rows.
        """
        try:
            content = bytes(''.join(rows), 'utf-8')
            with self.client.open(file_path, 'wb') as file:
                file.write(content)
        except Exception:
            raise FileNotFoundError(f'Failed to write file [{file_path}] in data lake.')
        
    def append_parquet(self, df, save_path, schema=None):
        """
        Append dataframe content in data lake parquet file.
        Parameters
        ----------
        df: pandas.core.frame.DataFrame
            - Pandas dataframe.
        
        save_path: string
            - Path to save content.
        
        schema: object
            - Parquet file schema.
        """
        buffer = io.BytesIO()
        try:
            if self.client.exists(save_path):
                adl_df = self.read_parquet_file(save_path)
                adl_df = adl_df.append(df)
                adl_df.drop_duplicates(inplace=True, ignore_index=True)
                adl_df.to_parquet(buffer)
            else:
                df.to_parquet(buffer)
            with self.client.open(save_path, 'wb') as parquet_file:
                parquet_file.write(buffer.getbuffer())

                df = pd.read_parquet(buffer)

        except Exception:
            raise RuntimeError(f'File append failed [{save_path}]')

    def backup_file(self, file_path, backup_file_path):
        """
        Create backup of the file in backup path.
        Parameters
        ----------
        file_path: string
            - Data lake path of the file.
        
        backup_file_path: string
            - Data lake path to backup the file.
        """
        self._create_path_if_not_exists(backup_file_path)
        # Backup file
        try:
            if self.client.exists(file_path):
                if self.client.exists(backup_file_path):
                    self.client.rm(backup_file_path)
                self.client.mv(file_path, backup_file_path)
        except Exception:
            raise RuntimeError(f'Failed to back up the file [{file_path}]')

    def get_files_by_filter(self, directory: str, filter_text = '', filter_extension = ''):
        """
        Return files by a filter of file name or file extension.
        Parameters
        ----------
        directory: string
            - Data lake directory.
        filter_text: string
            - Filter file name.
        filter_extension: string
            - Filter file extension.
        """
        files = self.list_dir(directory)

        try:
            if filter_text:
                files = list(filter(lambda f: filter_text.strip().lower() in f.strip().lower(), files))
            
            if filter_extension:
                files = list(filter(lambda f: f.strip().lower().endswith(filter_extension), files))
        except Exception:
            raise RuntimeError(f'Failed to filter directory [{directory}]')
        return files

    def get_encoding(self, file_path):
        """
        Return encoding of the file.
        Parameter
        ---------
        file_path: string
            - Data lake path of file.
        Return
        ------
        string
            - Encoding type of file.      
        """
        file_encoding = 'utf-8'
        try:
            with self.client.open(file_path) as file:
                file_content = file.read()
                file_detect = chardet.detect(file_content)
                file_encoding = file_detect['encoding']

            return file_encoding
        except FileNotFoundError as e:
            raise FileNotFoundError(f'Failed to detect file encoding [{e}]')

    def read_input_file(self, workdir, file_name, only_required=False, encoding = None, engine = None, header = None):
        required_columns = None
        try:
            file_schema = self.files_schema.get_file_schema(file_name)
            file_path = eval(f'f"{workdir}{file_schema["path"]}"')
            file_extension = pathlib.Path(file_path).suffix
        except Exception:
            raise RuntimeError(f'Failed to detect file extension [{file_name}]')

        try:
            columns_type = self.files_schema.get_required_columns_type(file_name)
        except Exception:
            raise RuntimeError(f'Failed to get column typing for the file [{file_name}]')

        try:
            required_columns = None if not only_required else self.files_schema.get_required_columns(file_name)
        except Exception:
            raise RuntimeError(f'Failed to get required file columns [{file_name}]')

        if not self.verify_file_exists(file_path):
            raise FileNotFoundError(f'File not found [{file_path}]')
        
        if file_extension == '.csv':
            dataframe = self.read_csv_file_on_dataframe(
                file_path,
                encoding = encoding,
                engine = engine,
                header = file_schema['header'],
                use_cols = required_columns,
                dtype = columns_type)
        elif file_extension == '.xlsb':
            dataframe = self.read_xlsb_file_on_dataframe(
                file_path, 
                sheet_name=file_schema['sheet'], 
                header=file_schema['header'],
                use_cols=required_columns,
                dtype=columns_type
            )
        elif file_extension == '.xlsx':
            dataframe = self.read_xlsx_file_on_dataframe(
                file_path, 
                sheet_name=file_schema['sheet'], 
                header=file_schema['header'],
                use_cols=required_columns,
                dtype=columns_type
            )
        elif file_extension == '.xls':
            dataframe = self.read_excel_file(
                file_path, 
                sheet_name=file_schema['sheet'], 
                header=file_schema['header'],
                usecols=required_columns,
                dtype=columns_type
            )
        return dataframe