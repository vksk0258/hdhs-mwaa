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
    dag_id="dag_CDC_MART_PHYS_DIST_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0010_PHYS_DIST_01","MART프로시져"]
) as dag:
    task_SP_RPD_DPRCH_EUPDN_INF = PythonOperator(
        task_id="task_SP_RPD_DPRCH_EUPDN_INF",
        python_callable=execute_procedure,
        op_args=["SP_RPD_DPRCH_EUPDN_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RPD_DPRCH_ITEM_STCK_INF = PythonOperator(
        task_id="task_SP_RPD_DPRCH_ITEM_STCK_INF",
        python_callable=execute_procedure,
        op_args=["SP_RPD_DPRCH_ITEM_STCK_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PPD_OSHP_TKTM_DYS_FCT = PythonOperator(
        task_id="task_SP_PPD_OSHP_TKTM_DYS_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PPD_OSHP_TKTM_DYS_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PAR_INSU_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_PAR_INSU_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PAR_INSU_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PAR_SO_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_PAR_SO_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PAR_SO_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_ITEM_ANAL_FCT = PythonOperator(
        task_id="task_SP_RIA_ITEM_ANAL_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_ITEM_ANAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )


    task_SP_PCU_CUST_ANAL_FCT = PythonOperator(
        task_id="task_SP_PCU_CUST_ANAL_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_ANAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PCU_CUST_ANAL_FCT_01 = PythonOperator(
        task_id="task_SP_PCU_CUST_ANAL_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_ANAL_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PCU_CUST_ANAL_FCT_02 = PythonOperator(
        task_id="task_SP_PCU_CUST_ANAL_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_ANAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PMA_COPN_ANAL_DLU_FCT_01 = PythonOperator(
        task_id="task_SP_PMA_COPN_ANAL_DLU_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_PMA_COPN_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PMA_COPN_ANAL_DLU_FCT_02 = PythonOperator(
        task_id="task_SP_PMA_COPN_ANAL_DLU_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PMA_COPN_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_ITEM_ANAL_FCT_02 = PythonOperator(
        task_id="task_SP_RIA_ITEM_ANAL_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_RIA_ITEM_ANAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PMA_EVNT_ANAL_DLU_FCT = PythonOperator(
        task_id="task_SP_PMA_EVNT_ANAL_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PMA_EVNT_ANAL_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RPD_DPRCH_EUPDN_INF >>\
    task_SP_RPD_DPRCH_ITEM_STCK_INF >>\
    task_SP_PPD_OSHP_TKTM_DYS_FCT >>\
    task_SP_PAR_INSU_ARLT_DLU_FCT >>\
    task_SP_PAR_SO_ARLT_DLU_FCT >>\
    task_SP_RIA_ITEM_ANAL_FCT >>\
    task_SP_PCU_CUST_ANAL_FCT >>\
    task_SP_PCU_CUST_ANAL_FCT_01 >>\
    task_SP_PCU_CUST_ANAL_FCT_02 >>\
    task_SP_PMA_COPN_ANAL_DLU_FCT_01 >>\
    task_SP_PMA_COPN_ANAL_DLU_FCT_02 >>\
    task_SP_RIA_ITEM_ANAL_FCT_02 >>\
    task_SP_PMA_EVNT_ANAL_DLU_FCT



