from hurricane.schemas.files_schema import FilesSchema
from hurricane.utils.datalake import Datalake

class RawValidations:
    def process_raw_validations(**args):
        dag_id  = args["dag_id"]
        adl     = args["adl"]
        workdir = args["workdir"]
        config  = args["config_json"]

        files_schema = FilesSchema(dag_id)
        datalake     = Datalake(adl, dag_id)
                
        for key in config:
            print(f"-------------------------------------------------------------")
            print(f"Check this files and columns schema with based in key [{key}]")
            print(f"-------------------------------------------------------------")

            raw_path = f"{workdir}{key['input_path']}"
            files = datalake.list_dir(raw_path)
            if not files:
                print(f"There are no files to process [{key['input_path']}]")
                continue

            for file in files:
                print("----------------------------------------------")
                print(f"Check this file [{file}]")
                print("----------------------------------------------")
                dataframe = datalake.read_csv_file_on_dataframe(file, encoding = "utf-16", engine = 'python', header = 0) 
                columns = dataframe.columns.tolist()
                is_valid, diff = files_schema.validate_file_schema(key, columns)
                if not is_valid:
                    raise ValueError(f"The {diff} columns do not exist in the file [{file}]")