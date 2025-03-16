from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from common.notify_error_functions import notify_api_on_error
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_MONTHLY_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_MONTHLY_02",
    schedule_interval=None,
    tags=["현대홈쇼핑","MONTHLY","MART프로시져"]
) as dag:
    @task.branch(task_id='branching')
    def check_monthly(p_end):
        if p_end[6:8] == '02': # 7번째(인덱스 6)부터 2글자
            return 'task_SP_CU_CALL_LDIN_INF_TO_HDHS'


    task_SP_CU_CALL_LDIN_INF_TO_HDHS = PythonOperator(
        task_id="task_SP_CU_CALL_LDIN_INF_TO_HDHS",
        python_callable=execute_procedure,
        op_args=["SP_CU_CALL_LDIN_INF_TO_HDHS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    check_monthly(p_end) >> task_SP_CU_CALL_LDIN_INF_TO_HDHS