from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_1200_CP_SMR_MST_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

with DAG(
    dag_id="dag_CDC_MART_CAMP_CP_SMR_MST",
    schedule_interval='0 3 * * *',
    start_date=pendulum.datetime(2025, 2, 20, tz="Asia/Seoul"),
    tags=["현대홈쇼핑","dag_DD01_1200_CP_SMR_MST_01"]
) as dag:
    task_SP_CP_SMR_MST = PythonOperator(
        task_id="task_SP_CP_SMR_MST",
        python_callable=execute_procedure,
        op_args=["SP_CP_SMR_MST", p_start, p_end, 'conn_snowflake_api'],
        trigger_rule="all_done"
    )

    task_SP_CP_SMR_MST