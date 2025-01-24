from airflow import DAG
from informixdb_to_s3_incremental_load_operator import InformixdbToS3IncrementalLoadOperator
import json
import boto3
import pendulum
import datetime

KST = pendulum.timezone("Asia/Seoul")

# S3 JSON 파라미터 로드
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

# Informix 컬럼 정의
dagent_columns = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL", "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME", "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS", "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME", "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4", "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2", "I_OTHERSTBYTIME", "I_AUXSTBYTIME"]
# Snowflake 키 컬럼 정의
dagent_primary_keys = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL"]
hagent_primary_keys = ["ROW_DATE","STARTTIME", "INTRVL", "ACD", "SPLIT", "EXTENSION", "LOGID", "RSV_LEVEL"]

hagent_columns = [
    "ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL",
    "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME",
    "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME",
    "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS",
    "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2",
    "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME",
    "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME",
    "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME",
    "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS",
    "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME",
    "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS",
    "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME",
    "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4",
    "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2",
    "I_OTHERSTBYTIME", "I_AUXSTBYTIME"
]

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_01",
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=2400),
        tags=["현대홈쇼핑","DD01_0030_DAILY_MAIN"]
) as dag:

    task_DWCT_DAGENT_c_01 = InformixdbToS3IncrementalLoadOperator(
        task_id="task_DWCT_DAGENT_c_01",
        conn_id = "conn_informix_locus1",
        table = "ODS_CMS.DWCT_DAGENT",
        columns = dagent_columns,
        not_null_columns = dagent_primary_keys,
        p_start=params.get("$$P_START"),
        p_end=params.get("$$P_END"),
        batch_size = 100000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_DWCT_HAGENT_c_01 = InformixdbToS3IncrementalLoadOperator(
        task_id = "task_DWCT_HAGENT_c_01",
        conn_id = "conn_informix_locus1",
        table = "ODS_CMS.DWCT_HAGENT",
        columns = hagent_columns,
        not_null_columns = hagent_primary_keys,
        p_start=params.get("$$P_START"),
        p_end=params.get("$$P_END"),
        batch_size = 50000,
        retries = 10,
        retry_delay = datetime.timedelta(seconds=10)
    )

        # DAG 실행
    task_DWCT_DAGENT_c_01 >> task_DWCT_HAGENT_c_01



