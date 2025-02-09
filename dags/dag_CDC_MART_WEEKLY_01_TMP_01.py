from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
import boto3
import json

parent_dir = "120_MART"
parent_dag = "dag_WW01_MON_0630_WEEKLY_01"

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_WW01_MON_0630_WEEKLY_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")


with DAG(
    dag_id="dag_CDC_MART_WEEKLY_01_TMP_01",
    schedule_interval=None,
    tags=[parent_dir, parent_dag, "현대홈쇼핑"]
) as dag:
    task_SP_HES_EXP_SWRT_DTL_TMP = PythonOperator(
        task_id="task_SP_HES_EXP_SWRT_DTL_TMP",
        python_callable=execute_procedure,
        op_args=["SP_HES_EXP_SWRT_DTL_TMP", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_HES_EXP_SWRT_MLB_DTL_TMP = PythonOperator(
        task_id="task_SP_HES_EXP_SWRT_MLB_DTL_TMP",
        python_callable=execute_procedure,
        op_args=["SP_HES_EXP_SWRT_MLB_DTL_TMP", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_HES_EXP_SWRT_ONLN_DTL_TMP = PythonOperator(
        task_id="task_SP_HES_EXP_SWRT_ONLN_DTL_TMP",
        python_callable=execute_procedure,
        op_args=["SP_HES_EXP_SWRT_ONLN_DTL_TMP", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_UPNT_JOIN_RATE_DTL = PythonOperator(
        task_id="task_SP_RCU_UPNT_JOIN_RATE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RCU_UPNT_JOIN_RATE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    [task_SP_HES_EXP_SWRT_DTL_TMP, task_SP_HES_EXP_SWRT_MLB_DTL_TMP] >> task_SP_RAR_EXP_SWRT_ONLN_DTL