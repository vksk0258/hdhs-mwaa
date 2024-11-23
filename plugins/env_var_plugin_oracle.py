from airflow.plugins_manager import AirflowPlugin
import os


os.environ["LD_LIBRARY_PATH"]='/usr/local/airflow/plugins/instantclient_19_25'
os.environ["DPI_DEBUG_LEVEL"]="64"
os.environ["PATH"] = os.getenv("PATH") + ":/usr/local/airflow/plugins/instantclient_19_25"

class EnvVarPlugin(AirflowPlugin):
    name = 'env_var_plugin'