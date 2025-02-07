from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0400_CMS_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_CMS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0330","MART프로시져"]
) as dag:
    task_RCA_MDA_AREA_CALL_DLU_FCT = PythonOperator(
        task_id="task_RCA_MDA_AREA_CALL_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RCA_MDA_AREA_CALL_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCA_MDA_AREA_CALL_HOU_FCT = PythonOperator(
        task_id="task_SP_RCA_MDA_AREA_CALL_HOU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RCA_MDA_AREA_CALL_HOU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_RCA_MDA_AREA_CALL_DLU_FCT >> task_SP_RCA_MDA_AREA_CALL_HOU_FCT