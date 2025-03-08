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
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")


# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_VLID_TERM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0010_VLID_TERM_01","MART프로시져"]
) as dag:
    task_SP_BCU_CUST_STAT_MST = PythonOperator(
        task_id="task_SP_BCU_CUST_STAT_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_STAT_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL = PythonOperator(
        task_id="task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_VLID_TERM_EMAIL_DTL", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done"
    )

    task_SP_DWCU_CUST_SMS_DROP = PythonOperator(
        task_id="task_SP_DWCU_CUST_SMS_DROP",
        python_callable=execute_procedure,
        op_args=["SP_DWCU_CUST_SMS_DROP", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_DWCU_CUST_INSU_DROP = PythonOperator(
        task_id="task_SP_DWCU_CUST_INSU_DROP",
        python_callable=execute_procedure,
        op_args=["SP_DWCU_CUST_INSU_DROP", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done"
    )

    task_SP_BCU_CUST_STAT_MST >> \
    task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL >> \
    task_SP_DWCU_CUST_SMS_DROP >> \
    task_SP_DWCU_CUST_INSU_DROP





