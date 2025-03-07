from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from airflow.models import Variable
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = Variable.get("start")
p_end = Variable.get("end")


with DAG(
    dag_id="dag_CDC_MART_DAILY_ARLT_01",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑","dag_DD01_0630_DAILY_BROAD_01","MART프로시져"]
) as dag:

    task_SP_RAR_RNTL_ARLT_DTL = PythonOperator(
        task_id="task_SP_RAR_RNTL_ARLT_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_RNTL_ARLT_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RAR_RNTL_ARLT_DTL
