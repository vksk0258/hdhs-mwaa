from airflow import DAG
from informixdb_to_s3_incremental_load_operator import InformixdbToS3IncrementalLoadOperator
from airflow.operators.python import PythonOperator
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

dsplit_columns = ['ROW_DATE, ACD, SPLIT, I_STAFFTIME, I_AVAILTIME, I_ACDTIME, I_ACWTIME, I_ACWOUTTIME, I_ACWINTIME, I_AUXTIME, I_AUXOUTTIME, I_AUXINTIME, I_OTHERTIME, MAXSTAFFED, ACWINCALLS, ACWINTIME, AUXINCALLS, AUXINTIME, ACWOUTCALLS, ACWOUTTIME, ACWOUTOFFCALLS, ACWOUTOFFTIME, ACWOUTADJCALLS, AUXOUTCALLS, AUXOUTTIME, AUXOUTOFFCALLS, AUXOUTOFFTIME, AUXOUTADJCALLS, EVENT1, EVENT2, EVENT3, EVENT4, EVENT5, EVENT6, EVENT7, EVENT8, EVENT9, ASSISTS, INFLOWCALLS, ACDCALLS, ANSTIME, ACDTIME, ACWTIME, O_ACDCALLS, O_ACDTIME, O_ACWTIME, ACDCALLS1, ACDCALLS2, ACDCALLS3, ACDCALLS4, ACDCALLS5, ACDCALLS6, ACDCALLS7, ACDCALLS8, ACDCALLS9, ACDCALLS10, BACKUPCALLS, HOLDCALLS, HOLDTIME, HOLDABNCALLS, TRANSFERRED, CONFERENCE, ABNCALLS, ABNTIME, ABNCALLS1, ABNCALLS2, ABNCALLS3, ABNCALLS4, ABNCALLS5, ABNCALLS6, ABNCALLS7, ABNCALLS8, ABNCALLS9, ABNCALLS10, DEQUECALLS, DEQUETIME, BUSYCALLS, BUSYTIME, DISCCALLS, DISCTIME, OUTFLOWCALLS, OUTFLOWTIME, INTERFLOWCALLS, LOWCALLS, MEDCALLS, HIGHCALLS, TOPCALLS, ACCEPTABLE, SERVICELEVEL, PERIOD1, PERIOD2, PERIOD3, PERIOD4, PERIOD5, PERIOD6, PERIOD7, PERIOD8, PERIOD9, MAXINQUEUE, MAXOCWTIME, CALLSOFFERED, PERIODCHG, SVCLEVELCHG, I_RINGTIME, RINGTIME, RINGCALLS, ABNRINGCALLS, O_ABNCALLS, O_OTHERCALLS, DA_ACWINCALLS, DA_ACWINTIME, DA_ACWOCALLS, DA_ACWOTIME, NOANSREDIR, INCOMPLETE, ACDAUXOUTCALLS, I_ACDAUX_OUTTIME, I_ACDAUXINTIME, I_ACDOTHERTIME, PHANTOMABNS, OTHERCALLS, OTHERTIME, SLVLABNS, SLVLOUTFLOWS, I_ARRIVED, I_AUXTIME0, I_AUXTIME1, I_AUXTIME2, I_AUXTIME3, I_AUXTIME4, I_AUXTIME5, I_AUXTIME6, I_AUXTIME7, I_AUXTIME8, I_AUXTIME9, I_DA_ACDTIME, I_DA_ACWTIME, I_TAVAILTIME, I_TAUXTIME, MAXTOP, I_NORMTIME, I_OL1TIME, I_OL2TIME, I_TOTHERTIME, MAX_TOT_PERCENTS, ACDCALLS_R1, ACDCALLS_R2, I_ACDTIME_R1, I_ACDTIME_R2, I_ACWTIME_R1, I_ACWTIME_R2, I_RINGTIME_R1, I_RINGTIME_R2, I_OTHERTIME_R1, I_OTHERTIME_R2, I_AUXTIME_R1, I_AUXTIME_R2, I_OTHERSTBYTIME_R1, I_OTHERSTBYTIME_R2, I_AUXSTBYTIME_R1, I_AUXSTBYTIME_R2, I_BEHINDTIME, I_AUTORESERVETIME, TARGETPERCENT, TARGETPCTCHG, TARGETSECONDS, TARGETSECCHG, TARGETACDCALLS, TARGETABNS, TARGETOUTFLOWS']
hsplit_columns = [
    "ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "I_STAFFTIME", "I_AVAILTIME", "I_ACDTIME", "I_ACWTIME",
    "I_ACWOUTTIME", "I_ACWINTIME", "I_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "MAXSTAFFED",
    "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS",
    "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME",
    "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9",
    "ASSISTS", "INFLOWCALLS", "ACDCALLS", "ANSTIME", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME",
    "ACDCALLS1", "ACDCALLS2", "ACDCALLS3", "ACDCALLS4", "ACDCALLS5", "ACDCALLS6", "ACDCALLS7", "ACDCALLS8",
    "ACDCALLS9", "ACDCALLS10", "BACKUPCALLS", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED",
    "CONFERENCE", "ABNCALLS", "ABNTIME", "ABNCALLS1", "ABNCALLS2", "ABNCALLS3", "ABNCALLS4", "ABNCALLS5",
    "ABNCALLS6", "ABNCALLS7", "ABNCALLS8", "ABNCALLS9", "ABNCALLS10", "DEQUECALLS", "DEQUETIME", "BUSYCALLS",
    "BUSYTIME", "DISCCALLS", "DISCTIME", "OUTFLOWCALLS", "OUTFLOWTIME", "INTERFLOWCALLS", "LOWCALLS", "MEDCALLS",
    "HIGHCALLS", "TOPCALLS", "ACCEPTABLE", "SERVICELEVEL", "PERIOD1", "PERIOD2", "PERIOD3", "PERIOD4", "PERIOD5",
    "PERIOD6", "PERIOD7", "PERIOD8", "PERIOD9", "MAXINQUEUE", "MAXOCWTIME", "CALLSOFFERED", "PERIODCHG",
    "SVCLEVELCHG", "I_RINGTIME", "RINGTIME", "RINGCALLS", "ABNRINGCALLS", "O_ABNCALLS", "O_OTHERCALLS",
    "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS",
    "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "OTHERCALLS", "OTHERTIME",
    "SLVLABNS", "SLVLOUTFLOWS", "I_ARRIVED", "I_AUXTIME0", "I_AUXTIME1", "I_AUXTIME2", "I_AUXTIME3",
    "I_AUXTIME4", "I_AUXTIME5", "I_AUXTIME6", "I_AUXTIME7", "I_AUXTIME8", "I_AUXTIME9", "I_DA_ACDTIME",
    "I_DA_ACWTIME", "I_TAVAILTIME", "I_TAUXTIME", "MAXTOP", "I_NORMTIME", "I_OL1TIME", "I_OL2TIME", "I_TOTHERTIME",
    "MAX_TOT_PERCENTS", "ACDCALLS_R1", "ACDCALLS_R2", "I_ACDTIME_R1", "I_ACDTIME_R2", "I_ACWTIME_R1",
    "I_ACWTIME_R2", "I_RINGTIME_R1", "I_RINGTIME_R2", "I_OTHERTIME_R1", "I_OTHERTIME_R2", "I_AUXTIME_R1",
    "I_AUXTIME_R2", "I_OTHERSTBYTIME_R1", "I_OTHERSTBYTIME_R2", "I_AUXSTBYTIME_R1", "I_AUXSTBYTIME_R2",
    "I_BEHINDTIME", "I_AUTORESERVETIME", "TARGETPERCENT", "TARGETPCTCHG", "TARGETSECONDS", "TARGETSECCHG",
    "TARGETACDCALLS", "TARGETABNS", "TARGETOUTFLOWS"
]

hsplit_primary_keys = ["ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "I_ACWINTIME"]

def task_DWCT_HSPLIT_c_01 (table,columns, not_null_columns, **kwargs):
    import os
    from airflow.providers.jdbc.hooks.jdbc import JdbcHook
    from airflow.models import Variable
    import pandas as pd
    import pendulum

    KST = pendulum.timezone("Asia/Seoul")
    S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
    informix_jdbc = Variable.get("informix_jdbc")
    schema, table_name = table.split('.')

    TMP_DIR = f"/tmp/oracle_initial/{table_name}"
    os.makedirs(TMP_DIR, exist_ok=True)

    informixdb_hook = JdbcHook(jdbc_conn_id="conn_informix_locus1", driver_path=informix_jdbc,
                               driver_class='com.informix.jdbc.IfxDriver')
    informixdb_connection = informixdb_hook.get_conn()

    chunk_index = 1

    current_time = kwargs['data_interval_end'].in_tz(KST)
    date_folder = current_time.strftime('%Y/%m/%d')

    save_name = current_time.strftime('%Y%m%d')

    batch_size = 50000

    while True:
        # SQL 실행 (OFFSET-FETCH 또는 ROWNUM 사용)
        offset = (chunk_index - 1) * batch_size

        # 각 컬럼에 대해 NOT NULL 조건을 생성
        not_null_conditions = " AND ".join([f"{key} IS NOT NULL" for key in not_null_columns])

        query = f"""
                            SELECT skip {offset} FIRST {offset + batch_size} {', '.join(columns)}
                            FROM {table_name.split('_')[1]}
                            WHERE {not_null_conditions}
                        """
        print(query)

        df = pd.read_sql(query, informixdb_connection)
        if df.empty:
            break

        file_name = f"{TMP_DIR}/{table_name}_{save_name}_{chunk_index:08d}.parquet"
        s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{save_name}_{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} {s3_path}")
        print(f"Uploaded {file_name} to {s3_path}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        offset += batch_size
        chunk_index += 1

    informixdb_connection.close()
    print("커넥션 종료")

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
        batch_size = 50000,
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

    task_DWCT_HSPLIT_c_01 = PythonOperator(
        task_id = "task_DWCT_HSPLIT_c_01",
        python_callable=task_DWCT_HSPLIT_c_01,
        op_args=['ODS_CMS.DWCT_HSPLIT',hsplit_columns,hsplit_primary_keys,],
        retries = 10,
        retry_delay = datetime.timedelta(seconds=10)
    )

        # DAG 실행
    task_DWCT_DAGENT_c_01 >> task_DWCT_HAGENT_c_01 >> task_DWCT_HSPLIT_c_01



