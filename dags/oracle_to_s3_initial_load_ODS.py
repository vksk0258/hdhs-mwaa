from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import datetime
from airflow.models import Variable
import os
import pandas as pd
import boto3
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# 환경 설정
client_path = Variable.get("client_path")
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
TMP_DIR = "/tmp/oracle_initial"
ORACLE_CONN_ID = "conn_oracle_H2O"

dagent_columns = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL", "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME", "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS", "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME", "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4", "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2", "I_OTHERSTBYTIME", "I_AUXSTBYTIME"]
dsplit_columns = ["ROW_DATE", "ACD", "SPLIT", "I_STAFFTIME", "I_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "I_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "MAXSTAFFED", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "INFLOWCALLS", "ACDCALLS", "ANSTIME", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "ACDCALLS1", "ACDCALLS2", "ACDCALLS3", "ACDCALLS4", "ACDCALLS5", "ACDCALLS6", "ACDCALLS7", "ACDCALLS8", "ACDCALLS9", "ACDCALLS10", "BACKUPCALLS", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "ABNCALLS1", "ABNCALLS2", "ABNCALLS3", "ABNCALLS4", "ABNCALLS5", "ABNCALLS6", "ABNCALLS7", "ABNCALLS8", "ABNCALLS9", "ABNCALLS10", "DEQUECALLS", "DEQUETIME", "BUSYCALLS", "BUSYTIME", "DISCCALLS", "DISCTIME", "OUTFLOWCALLS", "OUTFLOWTIME", "INTERFLOWCALLS", "LOWCALLS", "MEDCALLS", "HIGHCALLS", "TOPCALLS", "ACCEPTABLE", "SERVICELEVEL", "PERIOD1", "PERIOD2", "PERIOD3", "PERIOD4", "PERIOD5", "PERIOD6", "PERIOD7", "PERIOD8", "PERIOD9", "MAXINQUEUE", "MAXOCWTIME", "CALLSOFFERED", "PERIODCHG", "SVCLEVELCHG", "I_RINGTIME", "RINGTIME", "RINGCALLS", "ABNRINGCALLS", "O_ABNCALLS", "O_OTHERCALLS", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "OTHERCALLS", "OTHERTIME", "SLVLABNS", "SLVLOUTFLOWS", "I_ARRIVED", "I_AUXTIME0", "I_AUXTIME1", "I_AUXTIME2", "I_AUXTIME3", "I_AUXTIME4", "I_AUXTIME5", "I_AUXTIME6", "I_AUXTIME7", "I_AUXTIME8", "I_AUXTIME9", "I_DA_ACDTIME", "I_DA_ACWTIME", "I_TAVAILTIME", "I_TAUXTIME", "MAXTOP", "I_NORMTIME", "I_OL1TIME", "I_OL2TIME", "I_TOTHERTIME", "MAX_TOT_PERCENTS", "ACDCALLS_R1", "ACDCALLS_R2", "I_ACDTIME_R1", "I_ACDTIME_R2", "I_ACWTIME_R1", "I_ACWTIME_R2", "I_RINGTIME_R1", "I_RINGTIME_R2", "I_OTHERTIME_R1", "I_OTHERTIME_R2", "I_AUXTIME_R1", "I_AUXTIME_R2", "I_OTHERSTBYTIME_R1", "I_OTHERSTBYTIME_R2", "I_AUXSTBYTIME_R1", "I_AUXSTBYTIME_R2", "I_BEHINDTIME", "I_AUTORESERVETIME", "TARGETPERCENT", "TARGETPCTCHG", "TARGETSECONDS", "TARGETSECCHG", "TARGETACDCALLS", "TARGETABNS", "TARGETOUTFLOWS"]

BATCH_SIZE = 1000000
TABLE_NAME_LIST = [
    "ODS_CTI.DWCT_CTI_REAL",
    "ODS_ALLI.AM_ALML_MD_VEN_INTL_SETUP_DTL",
    "ODS_ALLI.AM_ALML_INTL_EXCP_SETUP_DTL",
    "ODS_ALLI.AM_ALML_ITEM_INTL_DTL",
    "ODS_CMS.DWCT_DAGENT",
    "ODS_CMS.DWCT_DSPLIT",
    "ODS_CMS.DWCT_HAGENT",
    "ODS_CMS.DWCT_HSPLIT"
]

def file_exists_in_s3(bucket_name, key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return True
    except:
        return False

def process_table(table_name, batch_size, tmp_dir, s3_bucket_name,**kwargs):
    os.makedirs(tmp_dir, exist_ok=True)

    schema, table = table_name.split('.')
    oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID, thick_mode=True, thick_mode_lib_dir=client_path)
    postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hdhs_reading')

    chunk_index = 1
    s3_prefix = f"dw/{schema}/{table}/"

    while True:
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        if file_exists_in_s3(s3_bucket_name, s3_key):
            print(f"File {s3_key} already exists in S3. Skipping upload.")
            chunk_index += 1
            continue

        offset = (chunk_index - 1) * batch_size

        # if schema == "ODS_CMS":
        #     columns = dsplit_columns if table in ["DSPLIT", "HSPLIT"] else dagent_columns
        #     query = f"""
        #                     SELECT {', '.join(columns)}
        #                     FROM {table_name}
        #                     OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
        #                 """
        # else:
        #     query = f"""
        #                     SELECT *
        #                     FROM {table}
        #                     LIMIT {batch_size} OFFSET {offset}
        #                 """
        #
        # conn = oracle_hook.get_conn() if schema == "ODS_CMS" else postgres_hook.get_conn()\

        with oracle_hook.get_conn() as conn:

            query = f"""
                        SELECT *
                        FROM {table_name}
                        OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
                    """
            df = pd.read_sql(query, conn)


        if df.empty:
            print("No more data to process for this table.")
            break
        if schema == "ODS_CMS":
            df['ETL_DTM'] = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul")).strftime(
                '%Y-%m-%d %H:%M:%S')

        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"
        df.to_parquet(file_name, engine='pyarrow', index=False)
        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        os.remove(file_name)
        chunk_index += 1

    print("Processing complete for table:", table_name)


# DAG 정의
with DAG(
        dag_id="oracle_to_s3_initial_load_ODS",
        schedule_interval='10 0 * * *',
        start_date=pendulum.datetime(2025, 1, 15, tz="Asia/Seoul"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=2400),
        tags=["현대홈쇼핑", "초기적재"]
) as dag:
    tasks = []

    for table_name in TABLE_NAME_LIST:
        task = PythonOperator(
            task_id=f"process_{table_name.replace('.', '_')}",
            python_callable=process_table,
            op_args=[table_name, BATCH_SIZE, TMP_DIR, S3_BUCKET_NAME],
            retries=10,
            retry_delay=datetime.timedelta(seconds=10),
            trigger_rule="all_done"
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
