from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from operators.snowflake_to_s3_load_operator_v2 import SnowflakeToS3LoadOperatorV2
import datetime
import pandas as pd
import pendulum
import boto3
import json
import os

# Column definitions

RAR_REAL_SWRT_DTL_COLUMNS = [
    "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD", "ITEM_M_CSF_CD",
    "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD", "BROD_DT",
    "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM",
    "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM",
    "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY",
    "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT", "RGST_ID",
    "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM", "ETL_DTM"
]

RAR_REAL_SWRT_ETC_DTL_COLUMNS = [
    "BROD_MDA_GBCD", "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD",
    "ITEM_M_CSF_CD", "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD",
    "BROD_DT", "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM",
    "ITEM_S_CSF_NM", "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM",
    "DEPT_ORGN_NM", "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY",
    "CNCL_QTY", "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT",
    "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM",
    "ETL_DTM"
]
RAR_REAL_SWRT_ONLN_DTL_COLUMNS = [
    "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD", "ITEM_M_CSF_CD",
    "ITEM_S_CSF_CD", "ITEM_D_CSF _CD", "BRND_CD", "SLITM_CD", "BROD_DT",
    "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM",
    "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM",
    "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY",
    "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT", "RGST_ID",
    "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM",
    "ETL_DTM"
]
RAR_REAL_SWRT_ONLN_ETC_DTL_COLUMNS = [
    "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD", "ITEM_M_CSF_CD",
    "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD", "BROD_DT",
    "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM",
    "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM",
    "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY",
    "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT", "RGST_ID",
    "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM",
    "ETL_DTM"
]

# Constants
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"
TMP_DIR = "/tmp/incremental"
TABLE_NAME_LIST = [
    {"table": "DW_RM.RAR_REAL_SWRT_DTL", "columns": RAR_REAL_SWRT_DTL_COLUMNS},
    {"table": "DW_RM.RAR_REAL_SWRT_ETC_DTL", "columns": RAR_REAL_SWRT_ETC_DTL_COLUMNS},
    {"table": "DW_RM.RAR_REAL_SWRT_ONLN_DTL", "columns": RAR_REAL_SWRT_ONLN_DTL_COLUMNS},
    {"table": "DW_RM.RAR_REAL_SWRT_ONLN_ETC_DTL", "columns": RAR_REAL_SWRT_ONLN_ETC_DTL_COLUMNS},
]

# Load parameters from S3
s3 = boto3.client('s3')
PARAM_BUCKET_NAME = "hdhs-dw-mwaa-s3"
PARAM_KEY = "param/wf_DD01_0400_ON_DEMAND_01.json"
response = s3.get_object(Bucket=PARAM_BUCKET_NAME, Key=PARAM_KEY)
params = json.load(response['Body'])

# Parse time parameters
p_start = params.get('$$P_START')
p_end = params.get('$$P_END')

fm_p_start = datetime.datetime.strptime(f"{params.get('$$P_START')}000000", "%Y%m%d%H%M%S")
fm_p_end = datetime.datetime.strptime(f"{params.get('$$P_END')}235959", "%Y%m%d%H%M%S")

p_start_add_1d = fm_p_start + datetime.timedelta(days=1)
p_end_add_1d = fm_p_end + datetime.timedelta(days=1)

BATCH_SIZE = 100000


def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_ON_DEMAND_01_C",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:

    condition_query=f"""
                    WHERE APLY_DT >= '{p_start_add_1d.strftime('%Y-%m-%d')}' 
                    AND APLY_DT <= '{p_end_add_1d.strftime('%Y-%m-%d')}'
                    """

    task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01 = SnowflakeToS3LoadOperatorV2(
        task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01",
        conn_id="conn_snowflake_etl",
        table="DW_RM.RAR_REAL_SWRT_DTL",
        columns=RAR_REAL_SWRT_DTL_COLUMNS,
        condition_query=condition_query,
        batch_size=1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_RAR_REAL_SWRT_ETC_DTL_HDHS_c_01 = SnowflakeToS3LoadOperatorV2(
        task_id="task_RAR_REAL_SWRT_ETC_DTL_HDHS_c_01",
        conn_id="conn_snowflake_etl",
        table="DW_RM.RAR_REAL_SWRT_ETC_DTL",
        columns=RAR_REAL_SWRT_ONLN_DTL_COLUMNS,
        condition_query=condition_query,
        batch_size=1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS_c_01 = SnowflakeToS3LoadOperatorV2(
        task_id="task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS_c_01",
        conn_id="conn_snowflake_etl",
        table="DW_RM.RAR_REAL_SWRT_ONLN_DTL",
        columns=RAR_REAL_SWRT_ONLN_ETC_DTL_COLUMNS,
        condition_query=condition_query,
        batch_size=1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS_c_01 = SnowflakeToS3LoadOperatorV2(
        task_id="task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS_c_01",
        conn_id="conn_snowflake_etl",
        table="DW_RM.RAR_REAL_SWRT_ONLN_ETC_DTL",
        columns=RAR_REAL_SWRT_ETC_DTL_COLUMNS,
        condition_query=condition_query,
        batch_size=1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)


    task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01 >> \
    task_RAR_REAL_SWRT_ETC_DTL_HDHS_c_01 >> \
    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS_c_01 >> \
    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS_c_01






