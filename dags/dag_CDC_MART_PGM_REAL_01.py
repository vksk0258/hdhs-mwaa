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
    dag_id="dag_CDC_MART_PGM_REAL_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_TRUNC_MORF_BITM_SELL_REAL = PythonOperator(
        task_id="task_SP_TRUNC_MORF_BITM_SELL_REAL",
        python_callable=execute_procedure,
        op_args=["SP_TRUNC_MORF_BITM_SELL_REAL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_F015 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_D028 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_D056 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_D084 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_F015 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D028 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D056 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D084 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_EXP_ORD_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_D000 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_D000",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_02_D000 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_02_D000",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_F015 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D028 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D056 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D084 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_F015 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D028 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D056 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D084 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_REAl_SMR_BROD_01 = PythonOperator(
        task_id="task_SP_RAR_REAl_SMR_BROD_01",
        python_callable=execute_procedure,
        op_args=["SP_RAR_REAl_SMR_BROD", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_TRUNC_MORF_BITM_SELL_REAL >> task_SP_BITM_SELL_REAL_ORD_F015 >> task_SP_BITM_SELL_REAL_ORD_D028 >> task_SP_BITM_SELL_REAL_ORD_D056 >> task_SP_BITM_SELL_REAL_ORD_D084 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_F015 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D028 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D056 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D084 >> task_SP_BITM_SELL_EXP_ORD_D000 >> task_SP_RIA_BITM_ORD_EXP_FCT_D000 >> task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000 >> task_SP_RIA_BITM_ORD_EXP_FCT_02_D000 >> task_SP_RIA_BITM_ORD_REAL_FCT_F015 >> task_SP_RIA_BITM_ORD_REAL_FCT_D028 >> task_SP_RIA_BITM_ORD_REAL_FCT_D056 >> task_SP_RIA_BITM_ORD_REAL_FCT_D084 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_F015 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D028 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D056 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D084 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084 >> task_SP_RAR_REAl_SMR_BROD_01



