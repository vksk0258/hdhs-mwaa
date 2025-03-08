from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
import boto3
import json

# Load parameters from S3
s3 = boto3.client('s3')
PARAM_BUCKET_NAME = "hdhs-dw-mwaa-s3"
PARAM_KEY = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=PARAM_BUCKET_NAME, Key=PARAM_KEY)
params = json.load(response['Body'])

# Parse time parameters
p_start = params.get('$$P_START')
p_end = params.get('$$P_END')


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_VLID_TERM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    task_SP_TRUNCATE_FOR_DW = PythonOperator(
        task_id="task_SP_TRUNCATE_FOR_DW",
        python_callable=execute_procedure,
        op_args=["SP_TRUNCATE_FOR_DW", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_HDHS_CU_CUST_STAT_DTL_DW = PythonOperator(
        task_id="task_SP_HDHS_CU_CUST_STAT_DTL_DW",
        python_callable=execute_procedure,
        op_args=["SP_HDHS_CU_CUST_STAT_DTL_DW", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_TRUNCATE_FOR_DW >> task_SP_HDHS_CU_CUST_STAT_DTL_DW






