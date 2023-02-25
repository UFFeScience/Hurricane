from airflow.models import Variable

class AirflowVariable:
    def get_variable(self, variable):
        return Variable.get(variable)
