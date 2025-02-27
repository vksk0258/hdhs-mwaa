from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import datetime
from airflow.models import Variable
import os
import pandas as pd
import pendulum

KST = pendulum.timezone("Asia/Seoul")

client_path = Variable.get("client_path")

# 환경 설정
TMP_DIR = "/tmp/oracle_incremental"
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
ORACLE_CONN_ID = "conn_oracle_main"
TABLE_NAME_LIST = [
    "HDHS_OD.OD_STLM_INF_CRYPT",
    "HDHS_CU.CU_ARS_LDIN_MST_CRYPT",
    "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
    "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
    "HDHS_ECS.TEC_CONT_CORP_CRYPT"
]

def incremental_load(table, **kwargs):
    """증분 데이터 추출 및 S3 업로드"""
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID, thick_mode=True, thick_mode_lib_dir=client_path)
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()

    # Data Interval에서 시작 시간과 종료 시간을 가져옴
    previous_time = kwargs['data_interval_start']
    current_time = kwargs['data_interval_end']

    # 시간 포맷 지정
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    previous_time_str = previous_time.strftime('%Y-%m-%d %H:%M:%S')
    date_folder = current_time.strftime('%Y/%m/%d')
    time_identifier = current_time.strftime('%H%M%S')

    schema, table_name = table.split('.')
    print(f"Processing table: {table} from {previous_time_str} to {current_time_str}")

    if table_name != "TEC_CONT_CORP_CRYPT":
        query = f"""
            SELECT 'I' AS Op, t.*, t.CHG_DTM AS transact_id
            FROM {table} t
            WHERE t.REG_DTM = t.CHG_DTM
            AND CHG_DTM >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            AND CHG_DTM < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            UNION ALL
            SELECT 'U' AS Op, t.*, t.CHG_DTM AS transact_id
            FROM {table} t
            WHERE t.REG_DTM <> t.CHG_DTM
            AND CHG_DTM >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            AND CHG_DTM < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
        """
    else:
        query = f"""
            SELECT 'I' AS Op, t.*, t.MODIFY_DATE AS transact_id
            FROM {table} t
            WHERE t.INSERT_DATE = t.MODIFY_DATE
            AND MODIFY_DATE >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            AND MODIFY_DATE < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            UNION ALL
            SELECT 'U' AS Op, t.*, t.MODIFY_DATE AS transact_id
            FROM {table} t
            WHERE t.INSERT_DATE <> t.MODIFY_DATE
            AND MODIFY_DATE >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
            AND MODIFY_DATE < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
        """

    df = pd.read_sql(query, conn)

    if not df.empty:
        # date 타입 컬럼을 문자열로 변환
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        file_name = f"{TMP_DIR}/{table_name}_{current_time.strftime('%Y%m%d')}-{time_identifier}.parquet"
        s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{current_time.strftime('%Y%m%d')}-{time_identifier}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data saved to {file_name}")

        os.system(f"aws s3 cp {file_name} {s3_path}")
        print(f"Uploaded {file_name} to {s3_path}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")
    else:
        print(f"No data to process for table {table} in the given time window.")

    cursor.close()
    conn.close()

dag = DAG(
    dag_id='hdhs_oracle_to_s3_incremental_serial_tasks',
    description='Incremental load for multiple Oracle tables to S3 in sequence every hour (KST)',
    start_date=pendulum.datetime(2024, 12, 20, 16, 0, 0, tz="Asia/Seoul"),
    schedule_interval="0 * * * *",
    tags=['oracle', 's3', 'incremental']
)

# 각 테이블에 대한 태스크를 직렬로 실행하도록 설정
for table in TABLE_NAME_LIST:
    schema, table_name = table.split('.')
    task_id = f"incremental_load_{schema.lower()}_{table_name.lower()}"

    current_task = PythonOperator(
        task_id=task_id,
        python_callable=incremental_load,
        op_kwargs={"table": table},
        provide_context=True,
        dag=dag,
        trigger_rule='none_skipped',
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )
