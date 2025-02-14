from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
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

BATCH_SIZE = 500000
TABLE_NAME_LIST = [
    "ODS_ALLI.AM_ALML_MD_VEN_INTL_SETUP_DTL",
    "ODS_ALLI.AM_ALML_INTL_EXCP_SETUP_DTL",
    "ODS_TMS.TMS_APP_DEVICE_LIST",
    "ODS_TMS.TMS_APP_USER_LIST",
    "ODS_TMS.TMS_SITE_USER_LIST",
    "ODS_TMS.TMS_CAMP_CHN_INFO",
    "ODS_TMS.TMS_CAMP_SCHD_INFO",
    "ODS_TMS.TMS_CAMP_SEND_LIST_01",
    "ODS_TMS.TMS_CAMP_SEND_LIST_02",
    "ODS_TMS.TMS_CAMP_SEND_LIST_03",
    "ODS_TMS.TMS_CAMP_SEND_LIST_04",
    "ODS_TMS.TMS_CAMP_SEND_LIST_05",
    "ODS_TMS.TMS_CAMP_SEND_LIST_06",
    "ODS_TMS.TMS_CAMP_SEND_LIST_07",
    "ODS_TMS.TMS_CAMP_SEND_LIST_08",
    "ODS_TMS.TMS_CAMP_SEND_LIST_09",
    "ODS_TMS.TMS_CAMP_SEND_LIST_10",
    "ODS_TMS.TMS_CAMP_SEND_LIST_11",
    "ODS_TMS.TMS_CAMP_SEND_LIST_12",
    "ODS_INSU.B_PRE_DEAL_DB",
    "ODS_INSU.TB_CNTTMST",
    "ODS_INSU.TB_CNTTMST_HINS",
    "ODS_INSU.TB_CU_DORM_CMPL_REST"
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

    chunk_index = 1
    s3_prefix = f"dw/mwaa_etl_load/{schema}/{table}/"

    while True:
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        if file_exists_in_s3(s3_bucket_name, s3_key):
            print(f"File {s3_key} already exists in S3. Skipping upload.")
            chunk_index += 1
            continue

        offset = (chunk_index - 1) * batch_size

        query = f"""
                        SELECT *
                        FROM {table_name}
                        OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
                    """


        conn = oracle_hook.get_conn()
        df = pd.read_sql(query, conn)


        if df.empty:
            print("No more data to process for this table.")
            break

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
        schedule_interval=None,
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
