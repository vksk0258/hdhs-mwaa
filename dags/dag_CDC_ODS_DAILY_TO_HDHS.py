from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import numpy as np
import pandas as pd
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

# p_start = '20250218'
# p_end = '20250218'

KST = pendulum.timezone("Asia/Seoul")

reverse_condition_query = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

forward_condition_query = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') + 1
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

etl_conn_id = 'conn_snowflake_etl_temp'
load_conn_id = 'conn_snow_load'
etl_table = 'DW_HSIS.HES_RNTL_ARLT_DTL'
load_table = 'MWAA.HES_RNTL_ARLT_DTL'


with DAG(
    dag_id="dag_CDC_ODS_DAILY_HMALL_TO_HDHS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0330"]
) as dag:
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)


    python_task_1 = print_context('task_decorator 실행')