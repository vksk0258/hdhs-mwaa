from airflow import DAG
from operators.oracle_to_snowflake_truncate_insert_operator import OracleToSnowflakeTruncateInsertOperator
import datetime
from common.notify_error_functions import notify_api_on_error
import pendulum
import boto3
import json


# S3 JSON 파라미터 로드
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_HH02_20_ODS_HOURLY_HES_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

# Parse time parameters
p_start = params.get('$$P_START')
p_end = params.get('$$P_END')

columns = [
    "SMR_DT", "SLITM_CD", "SELL_MDA_GBCD", "BRND_CD", "MD_CD",
    "DEPT_ORGN_CD", "PART_ORGN_CD", "RNTL_ITEM_TYPE_GBCD",
    "SELL_AMT", "MM_PYM_AMT", "INSM_MTHS", "ORD_QTY",
    "EXP_SWRT", "ARLT_AMT", "ETL_DTM", "RGST_ID", "RGST_IP",
    "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"
]



condition_qeury =""

with DAG(
    dag_id="dag_CDC_ODS_HOURLY_HES_01",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑","dag_HH01_00_ODS_HOURLY_HES_01"]
) as dag:

    task_HES_RNTL_ARLT_DTL = OracleToSnowflakeTruncateInsertOperator(
        task_id = "task_HES_RNTL_ARLT_DTL",
        oracle_conn_id = "conn_oracle_main",
        snowflake_conn_id="conn_snowflake_etl",
        oracle_table = "HDHS_DW.HES_RNTL_ARLT_DTL",
        snowflake_table = "DW_HSIS.HES_RNTL_ARLT_DTL",
        columns = columns,
        condition_query = condition_qeury,
        batch_size = 200000,
        trigger_rule="all_done",
        on_failure_callback=notify_api_on_error
    )

    task_HES_RNTL_ARLT_DTL
