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
    dag_id="dag_CDC_MART_LEV_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_RDM_ALLI_REF_CH_DIM = PythonOperator(
        task_id="task_SP_RDM_ALLI_REF_CH_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_ALLI_REF_CH_DIM", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_CUST_REFI_CTEL_INF = PythonOperator(
        task_id="task_SP_BCU_CUST_REFI_CTEL_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_REFI_CTEL_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RDM_SELL_MDA_DIM = PythonOperator(
        task_id="task_SP_RDM_SELL_MDA_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_SELL_MDA_DIM", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_SMS_RCV_AGR_CUST_INF = PythonOperator(
        task_id="task_SP_BCU_SMS_RCV_AGR_CUST_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_SMS_RCV_AGR_CUST_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_PUSH_RCV_AGR_CUST_INF = PythonOperator(
        task_id="task_SP_BCU_PUSH_RCV_AGR_CUST_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_PUSH_RCV_AGR_CUST_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_EMAIL_ADR_REFI_CUST_MST = PythonOperator(
        task_id="task_SP_BCU_EMAIL_ADR_REFI_CUST_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_EMAIL_ADR_REFI_CUST_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_CUST_MST = PythonOperator(
        task_id="task_SP_BCU_CUST_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_CTPF_VACO_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_CTPF_VACO_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_CTPF_VACO_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_CTPF_VACO_DTL2 = PythonOperator(
        task_id="task_SP_BOD_ORD_CTPF_VACO_DTL2",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_CTPF_VACO_DTL2", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_PTC = PythonOperator(
        task_id="task_SP_BOD_ORD_PTC",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_PTC", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BIM_ITEM_MST = PythonOperator(
        task_id="task_SP_BIM_ITEM_MST",
        python_callable=execute_procedure,
        op_args=["SP_BIM_ITEM_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_DC_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_DC_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_DC_DTL", p_start, p_end, 'conn_snowflake_etl'],
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


    [task_SP_RDM_ALLI_REF_CH_DIM, task_SP_BCU_CUST_REFI_CTEL_INF, task_SP_RDM_SELL_MDA_DIM]

    task_SP_BCU_CUST_REFI_CTEL_INF >> [task_SP_BCU_SMS_RCV_AGR_CUST_INF, task_SP_BCU_EMAIL_ADR_REFI_CUST_MST]

    task_SP_BCU_SMS_RCV_AGR_CUST_INF >> task_SP_BCU_PUSH_RCV_AGR_CUST_INF

    [task_SP_BCU_PUSH_RCV_AGR_CUST_INF, task_SP_BCU_EMAIL_ADR_REFI_CUST_MST] >> task_SP_BCU_CUST_MST

    task_SP_RDM_SELL_MDA_DIM >> [task_SP_BOD_ORD_CTPF_VACO_DTL, task_SP_BIM_ITEM_MST]

    task_SP_BOD_ORD_CTPF_VACO_DTL >> task_SP_BOD_ORD_CTPF_VACO_DTL2 >> task_SP_BOD_ORD_PTC >> task_SP_BOD_ORD_DC_DTL

    [task_SP_RDM_ALLI_REF_CH_DIM, task_SP_BCU_CUST_MST, task_SP_BOD_ORD_DC_DTL, task_SP_BIM_ITEM_MST] >> task_ETL_DAILY_LOG


