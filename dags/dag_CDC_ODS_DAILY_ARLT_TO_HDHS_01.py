from airflow import DAG
from operators.oracle_to_snowflake_merge_operator import OracleToSnowflakeMergeOperator
import datetime
import pendulum
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

KST = pendulum.timezone("Asia/Seoul")

REVERSE_CONDITION_QUERY = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

FORWARD_CONDITION_QUERY = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') + 1
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

etl_conn_id = 'conn_snowflake_etl_temp'
load_conn_id = 'conn_snow_load'

with DAG(
    dag_id="dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0630_DAILY_BROAD_01","ODS","역방향"]
) as dag:
    task_HES_RNTL_ARLT_DTL_TO_HDHS = OracleToSnowflakeMergeOperator(
        task_id="task_HES_RNTL_ARLT_DTL_TO_HDHS",
        oracle_conn_id=etl_conn_id,
        snowflake_conn_id=load_conn_id,
        oracle_table="DW_HSIS.HES_RNTL_ARLT_DTL",
        snowflake_table="HDHS_DW.HES_RNTL_ARLT_DTL",
        columns=['*'],
        pk_columns=['SELL_MDA_GBCD', 'SLITM_CD', 'SMR_DT'],
        condition_query=REVERSE_CONDITION_QUERY,
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_HES_RNTL_ARLT_DTL = OracleToSnowflakeMergeOperator(
        task_id="task_HES_RNTL_ARLT_DTL",
        oracle_conn_id=load_conn_id,
        snowflake_conn_id=etl_conn_id,
        oracle_table="HDHS_DW.HES_RNTL_ARLT_DTL",
        snowflake_table="DW_HSIS.HES_RNTL_ARLT_DTL",
        columns=['*'],
        pk_columns=['SELL_MDA_GBCD', 'SLITM_CD', 'SMR_DT'],
        condition_query=FORWARD_CONDITION_QUERY,
        batch_size=200000,
        trigger_rule="all_done"
    )

    task_HES_RNTL_ARLT_DTL_TO_HDHS >> task_HES_RNTL_ARLT_DTL