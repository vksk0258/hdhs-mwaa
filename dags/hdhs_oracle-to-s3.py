import pandas as pd
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터
import os

# 설정 상수
client_path = Variable.get("client_path")
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
S3_PREFIX = "dw/HDHS_OD/OD_STLM_INF_CRYPT/"
ORACLE_CONN_ID = "conn_oracle_main"
TABLE_NAME = "OD_STLM_INF_CRYPT"
BATCH_SIZE = 1000000  # 한 파일당 저장할 행 수
TMP_DIR = "/tmp/oracle_to_s3"

MASKING_COLUMNS = {
    "OD_STLM_INF_CRYPT": ["OWN_CUST_NM", "CRD_VLID_TERM_YM"]
}

def mask_columns(df, table_name):
    """
    특정 테이블의 컬럼을 마스킹 처리합니다.
    """
    if table_name in MASKING_COLUMNS:
        for col in MASKING_COLUMNS[table_name]:
            if col in df.columns:
                df[col] = 'xxxx'
    return df

with DAG(
    dag_id="hdhs_oracle-to-s3",  # DAG의 고유 식별자
    schedule=None,  # DAG의 예약 일정 없음 (수동 실행)
    dagrun_timeout=datetime.timedelta(minutes=4000),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","검증"]  # DAG에 붙일 태그
) as dag:
    @task(task_id='oracle_to_s3_upload', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def oracle_to_s3_upload():
        """
        Oracle 데이터를 BATCH_SIZE 단위로 읽어오고 Parquet로 저장 후 S3에 업로드
        """
        # 임시 디렉토리 생성
        os.makedirs(TMP_DIR, exist_ok=True)

        # 파일 이름 카운터
        chunk_index = 1

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()

        while True:
            # S3에 해당 파일이 있는지 먼저 확인
            s3_key = f"{S3_PREFIX}LOAD{chunk_index:08d}.parquet"
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
                FROM HDHS_OD.{TABLE_NAME}
                OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
            """

            print(f"offset={offset}, BATCH_SIZE={BATCH_SIZE}, chunk_index={chunk_index}")
            print(f"query={query}")
            df = pd.read_sql(query, oracle_connection)

            print("read_sql pass")

            if df.empty:
                print("df empty 실행")
                break

            print("df empty pass")

            # 컬럼 마스킹 처리
            df = mask_columns(df, TABLE_NAME)

            file_name = f"{TMP_DIR}/LOAD{chunk_index:08d}.parquet"  # 파일 이름 형식

            # Parquet 저장
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


    oracle_to_s3_upload=oracle_to_s3_upload()
