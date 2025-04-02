from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, execute_procedure_no_dycl, log_etl_completion
from datetime import datetime, timedelta
from airflow.utils.task_group import TaskGroup
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
    dag_id="dag_CDC_MART_BROD_FCT_01_PROD",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져","PROD"]
) as dag:
    # Group 1
    with TaskGroup("grp_brod_anla_dlu") as grp_brod_anla_dlu:
        t1 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t5 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t6 = PythonOperator(task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4, t5, t6]

    # Group 2
    with TaskGroup("grp_shpl_brod_arlt_dlu") as grp_shpl_brod_arlt_dlu:
        t1 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t5 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t6 = PythonOperator(task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4, t5, t6]

    grp_brod_anla_dlu >> grp_shpl_brod_arlt_dlu






