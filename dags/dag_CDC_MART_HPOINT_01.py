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

# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_HPOINT_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0200_HPOINT_01","MART프로시져"]
) as dag:
    task_SP_PC_PTCO_BASKT_INF = PythonOperator(
        task_id="task_SP_PC_PTCO_BASKT_INF",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_BASKT_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_ITEM_MST = PythonOperator(
        task_id="task_SP_PC_PTCO_ITEM_MST",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_ITEM_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_ITEM_CTGR_INF = PythonOperator(
        task_id="task_SP_PC_PTCO_ITEM_CTGR_INF",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_ITEM_CTGR_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_MEM_INF = PythonOperator(
        task_id="task_SP_PC_PTCO_MEM_INF",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_MEM_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_SALE_DTL = PythonOperator(
        task_id="task_SP_PC_PTCO_SALE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_SALE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_ITEM_SALE_DTL = PythonOperator(
        task_id="task_SP_PC_PTCO_ITEM_SALE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_ITEM_SALE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PC_PTCO_SALE_STLM_DTL = PythonOperator(
        task_id="task_SP_PC_PTCO_SALE_STLM_DTL",
        python_callable=execute_procedure,
        op_args=["SP_PC_PTCO_SALE_STLM_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    [task_SP_PC_PTCO_BASKT_INF, task_SP_PC_PTCO_ITEM_MST, task_SP_PC_PTCO_MEM_INF, task_SP_PC_PTCO_SALE_DTL]

    task_SP_PC_PTCO_ITEM_MST >> task_SP_PC_PTCO_ITEM_CTGR_INF

    task_SP_PC_PTCO_SALE_DTL >> task_SP_PC_PTCO_ITEM_SALE_DTL >> task_SP_PC_PTCO_SALE_STLM_DTL
