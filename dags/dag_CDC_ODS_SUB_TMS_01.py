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


TMS_APP_USER_LIST_query = f"""
WHERE(
    UPT_DATE >= TO_DATE('{p_start}000000', 'YYYYMMDDHH24MISS') 
    AND UPT_DATE <= TO_DATE('{p_end}235959', 'YYYYMMDDHH24MISS') + 1
)
OR
(
    REG_DATE >= TO_DATE('{p_start}000000', 'YYYYMMDDHH24MISS') 
    AND REG_DATE <= TO_DATE('{p_end}235959', 'YYYYMMDDHH24MISS') + 1
)
"""

TMS_APP_DEVICE_LIST_columns = [
    "DEVICE_ID", "SITE_ID", "APP_GRP_ID", "APP_ID", "CUST_ID", "LOGIN_YN", "UUID",
    "SESS_FLAG", "TOKEN", "NOTI_FLAG", "BMKT_FLAG", "APP_VER", "OS", "OS_VER",
    "DEVICE", "DEL_YN", "REG_DATE", "UPT_DATE", "NIGHT_FLAG"
]

TMS_APP_USER_LIST_columns = [
    "APP_GRP_ID",
    "CUST_ID",
    "SITE_ID",
    "LAST_DEVICE_ID",
    "REG_DATE",
    "UPT_DATE"
]

TMS_SITE_USER_LIST_columns = [
    "SITE_ID", "CUST_ID", "CUST_NAME", "CUST_EMAIL", "CUST_PHONE",
    "INACTIVE_FLAG", "MKT_EMAIL_YN", "MKT_PUSH_YN", "MKT_SMS_YN",
    "BIRTHDAY", "LOCATION1", "LOCATION2", "GENDER",
    "DATA1", "DATA2", "DATA3", "DATA4",
    "FIRST_CHANNEL", "SECOND_CHANNEL", "THIRD_CHANNEL",
    "REG_DATE", "UPT_DATE"
]

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_TMS_01",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑","TMS", "ODS"]  # DAG에 붙일 태그
) as dag:

    task_TMS_APP_DEVICE_LIST_load = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_APP_DEVICE_LIST_load",
        oracle_conn_id = "conn_oracle_OCI",
        snowflake_conn_id = "conn_snow_load",
        oracle_table = "ODS_TMS.TMS_APP_DEVICE_LIST",
        snowflake_table = "ODS_TMS.TMS_APP_DEVICE_LIST_TEMP",
        columns = TMS_APP_DEVICE_LIST_columns,
        pk_columns = ['DEVICE_ID'],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 100000,
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TMS_APP_USER_LIST_load = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_APP_USER_LIST_load",
        oracle_conn_id = "conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_APP_USER_LIST",
        snowflake_table="ODS_TMS.TMS_APP_USER_LIST",
        columns = TMS_APP_USER_LIST_columns,
        pk_columns = ["APP_GRP_ID", "CUST_ID"],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 100000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TMS_SITE_USER_LIST_load = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_SITE_USER_LIST_load",
        oracle_conn_id = "conn_oracle_OCI",
        snowflake_conn_id="conn_snow_load",
        oracle_table="ODS_TMS.TMS_SITE_USER_LIST",
        snowflake_table="ODS_TMS.TMS_SITE_USER_LIST",
        columns = TMS_SITE_USER_LIST_columns,
        pk_columns = ["SITE_ID", "CUST_ID"],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 100000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )



    [task_TMS_APP_DEVICE_LIST_load , task_TMS_APP_USER_LIST_load]
    task_TMS_APP_USER_LIST_load >> task_TMS_SITE_USER_LIST_load
