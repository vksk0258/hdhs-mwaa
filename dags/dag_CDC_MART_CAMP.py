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

with DAG(
    dag_id="dag_CDC_MART_CAMP",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_RCU_CUST_PRFR_ITEM_L_CSF_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_PRFR_ITEM_L_CSF_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_PRFR_ITEM_L_CSF_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_BUY_CHRTR_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_BUY_CHRTR_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_BUY_CHRTR_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_BCU_CAMP_TRGT_CUST_MST = PythonOperator(
        task_id="task_SP_BCU_CAMP_TRGT_CUST_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CAMP_TRGT_CUST_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_CRM_DLU_KPI_FCT = PythonOperator(
        task_id="task_SP_RCU_CRM_DLU_KPI_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CRM_DLU_KPI_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_CP_SMR_MST = PythonOperator(
        task_id="task_SP_CP_SMR_MST",
        python_callable=execute_procedure,
        op_args=["SP_CP_SMR_MST", p_start, p_end, 'conn_snowflake_api'],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_PRFR_ITEM_L_CSF_INF >> task_SP_RCU_CUST_BUY_CHRTR_INF >> task_SP_BCU_CAMP_TRGT_CUST_MST >> task_SP_RCU_CRM_DLU_KPI_FCT >> task_SP_CP_SMR_MST