from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import datetime
from airflow.models import Variable
import os
import pandas as pd
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# 환경 설정
client_path = Variable.get("client_path")
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
TMP_DIR = "/tmp/oracle_initial"
ORACLE_CONN_ID_OCI = "conn_oracle_H2O"
ORACLE_CONN_ID_MAIN = "conn_oracle_main"
BATCH_SIZE = 500000
TABLE_NAME_LIST = [
    "HDHS_CU.CU_ARS_LDIN_MST_CRYPT",
    "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
    "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
    "HDHS_ECS.TEC_CONT_CORP_CRYPT"
]

# 테이블별 마스킹할 컬럼 정의
MASKING_COLUMNS = {
    "CU_ARS_LDIN_MST_CRYPT": ["TELI", "TEL", "CUST_NM", "ECRYPT_CRD_NO", "CRD_VLID_TERM_YM"],
    "TEC_CONT_CORP_CRYPT": ["ADDR", "VEN_TEL", "EMAIL", "CUST_PERS1_HP", "CUST_PERS2_NAME", "CUST_PERS2_TEL", "CUST_PERS2_HP", "CUST_PERS2_ID"],
    "OD_CRD_APRVL_LOG_CRYPT": ["ECRYT_CRD_NO", "VLID_TERM_YM"],
    "OD_HPNT_PAY_APRVL_DTL_CRYPT": ["MALL_ID"],
}

def mask_columns(df, table_name):
    """
    특정 테이블의 컬럼을 마스킹 처리합니다.
    """
    if table_name in MASKING_COLUMNS:
        for col in MASKING_COLUMNS[table_name]:
            if col in df.columns:
                df[col] = 'XXXX'
    return df

def process_table(table_name):
    """
    특정 테이블 데이터를 처리하고 S3에 업로드합니다.
    """
    os.makedirs(TMP_DIR, exist_ok=True)

    schema, table = table_name.split('.')

    # 커넥션 ID 선택
    if table_name in [
        "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
        "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
        "HDHS_ECS.TEC_CONT_CORP_CRYPT"
    ]:
        oracle_conn_id = ORACLE_CONN_ID_OCI
    else:
        oracle_conn_id = ORACLE_CONN_ID_MAIN

    oracle_hook = OracleHook(oracle_conn_id=oracle_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
    oracle_connection = oracle_hook.get_conn()

    chunk_index = 1
    s3_prefix = f"dw/{schema}/{table}/"

    while True:
        # S3에 해당 파일이 있는지 먼저 확인
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"

        res = os.system(f"aws s3 ls {s3_path} > /dev/null 2>&1")
        if res == 0:
            print(f"File {s3_key} already exists in S3. Skipping upload.")
            chunk_index += 1
            continue

        # SQL 실행 (OFFSET-FETCH 또는 ROWNUM 사용)
        offset = (chunk_index - 1) * BATCH_SIZE
        query = f"""
                SELECT * 
                FROM {table_name}
                OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
            """

        print(f"offset={offset}, BATCH_SIZE={BATCH_SIZE}, chunk_index={chunk_index}")
        print(f"query={query}")
        df = pd.read_sql(query, oracle_connection)

        if df.empty:
            print("No more data to process for this table.")
            break

        # 컬럼 마스킹 처리
        df = mask_columns(df, table)

        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        file_name = f"{TMP_DIR}/LOAD{chunk_index:08d}.parquet"  # 파일 이름 형식

        # CSV 저장
        df.to_parquet(file_name,engine='pyarrow', index=False)
        print(f"Chunk {chunk_index} saved to {file_name}")

        # S3 업로드
        os.system(f"aws s3 cp {file_name} {s3_path}")
        print(f"Uploaded {file_name} to {s3_path}")

        # 로컬 파일 삭제
        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    oracle_connection.close()
    print("커넥션 종료")

# DAG 정의
with DAG(
        dag_id="oracle_to_s3_initial_load",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=2400),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "초기적재"]  # DAG에 붙일 태그
) as dag:
    tasks = []

    for table_name in TABLE_NAME_LIST:
        task = PythonOperator(
            task_id=f"process_{table_name.replace('.', '_')}",
            python_callable=process_table,
            op_args=[table_name],
            retries=10,
            retry_delay=datetime.timedelta(seconds=10),
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks[i] >> tasks[i + 1]
