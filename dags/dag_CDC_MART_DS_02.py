from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from common.notify_error_functions import notify_api_on_error
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


with DAG(
    dag_id="dag_CDC_MART_DS_02",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_RPD_DLV_FORM_DLU_SMR = PythonOperator(
        task_id="task_SP_RPD_DLV_FORM_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RPD_DLV_FORM_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RPD_DLV_FORM_OSHP_DLU_FCT = PythonOperator(
        task_id="task_SP_RPD_DLV_FORM_OSHP_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RPD_DLV_FORM_OSHP_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )


    [task_SP_RPD_DLV_FORM_DLU_SMR,task_SP_RPD_DLV_FORM_OSHP_DLU_FCT] >> task_ETL_DAILY_LOG