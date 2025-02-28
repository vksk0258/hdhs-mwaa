from airflow import DAG
from operators.oracle_to_snowflake_merge_operator import OracleToSnowflakeMergeOperator
from operators.oracle_to_snowflake_initial_load_operator import OracleToSnowflakeInitialLoadOperator
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
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

TMS_CAMP_SCHD_INFO_columns = ["POST_ID", "SERVER_ID", "CHANNEL_TYPE", "MSG_ID", "JOB_STATUS", "CONTENT_CONF", "SPOOL_CONF", "OPEN_CHECK", "CLICK_CHECK", "SAFEMAIL_YN", "QUE_CLOSE_DATE", "TRACKING_CLOSE", "START_DATE", "END_DATE", "STOP_DATE", "RESTART_DATE", "RESTART_END_DATE", "ERR_START_DATE", "ERR_END_DATE", "TARGET_CNT", "PUSHED_CNT", "FILTER_CNT", "FAIL_CNT", "OPEN_CNT", "CLICK_CNT", "SAFEMAIL_STATE", "DIVIDE_SEND_USE_YN", "DIVIDE_CNT", "DIVIDE_MINUTE", "DIVIDE_SCHD_SEQ", "LAST_MEMBER_ID", "SPOOL_READ_COUNT", "APPROVAL_ID", "APPROVAL_DATE", "APPROVAL_REQ_ID", "APPROVAL_REQ_DATE", "REG_ID", "SWITCHED_CNT", "REQ_DATE", "SAFEMAIL_REGISTER_D", "REG_DATE", "UPT_DATE", "SAFEMAIL_CONTENT", "IF_ID", "RECOVERY_FLAG"]

oracle_conn_id = "conn_oracle_tms"


# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_TMS_01",  # DAG의 고유 식별자
        schedule_interval='20 0 * * *',
        start_date=pendulum.datetime(2025, 2, 24, tz="Asia/Seoul"),
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑","TMS", "ODS"]  # DAG에 붙일 태그
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    task_TMS_APP_DEVICE_LIST = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_APP_DEVICE_LIST",
        oracle_conn_id = oracle_conn_id,
        snowflake_conn_id = "conn_snow_load",
        oracle_table = "HDHS_TMS.TMS_APP_DEVICE_LIST",
        snowflake_table = "ODS_TMS.TMS_APP_DEVICE_LIST",
        columns = TMS_APP_DEVICE_LIST_columns,
        pk_columns = ['DEVICE_ID'],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 200000,
        trigger_rule="all_done"
    )

    task_TMS_APP_USER_LIST = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_APP_USER_LIST",
        oracle_conn_id = oracle_conn_id,
        snowflake_conn_id="conn_snow_load",
        oracle_table="HDHS_TMS.TMS_APP_USER_LIST",
        snowflake_table="ODS_TMS.TMS_APP_USER_LIST",
        columns = TMS_APP_USER_LIST_columns,
        pk_columns = ["APP_GRP_ID", "CUST_ID"],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 200000,
        trigger_rule="all_done"
    )

    task_TMS_SITE_USER_LIST = OracleToSnowflakeMergeOperator(
        task_id = "task_TMS_SITE_USER_LIST",
        oracle_conn_id = oracle_conn_id,
        snowflake_conn_id="conn_snow_load",
        oracle_table="HDHS_TMS.TMS_SITE_USER_LIST",
        snowflake_table="ODS_TMS.TMS_SITE_USER_LIST",
        columns = TMS_SITE_USER_LIST_columns,
        pk_columns = ["SITE_ID", "CUST_ID"],
        condition_query = TMS_APP_USER_LIST_query,
        batch_size = 200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_SCHD_INFO = OracleToSnowflakeInitialLoadOperator(
        task_id = "task_TMS_CAMP_SCHD_INFO",
        oracle_conn_id = oracle_conn_id,
        snowflake_conn_id="conn_snow_load",
        oracle_table = "HDHS_TMS.TMS_CAMP_SCHD_INFO",
        snowflake_table = "ODS_TMS.TMS_CAMP_SCHD_INFO",
        columns = ['*'],
        batch_size = 200000,
        trigger_rule="all_done"
    )

    task_TMS_CAMP_CHN_INFO = OracleToSnowflakeInitialLoadOperator(
        task_id="task_TMS_CAMP_CHN_INFO",
        oracle_conn_id =oracle_conn_id,
        snowflake_conn_id="conn_snow_load",
        oracle_table="HDHS_TMS.TMS_CAMP_CHN_INFO",
        snowflake_table="ODS_TMS.TMS_CAMP_CHN_INFO",
        columns=['*'],
        batch_size=200000,
        trigger_rule="all_done"
    )



    task_ETL_SCHEDULE_c_01 >> [task_TMS_APP_DEVICE_LIST , task_TMS_APP_USER_LIST, task_TMS_CAMP_SCHD_INFO]
    task_TMS_APP_USER_LIST >> task_TMS_SITE_USER_LIST
    task_TMS_CAMP_SCHD_INFO >> task_TMS_CAMP_CHN_INFO
