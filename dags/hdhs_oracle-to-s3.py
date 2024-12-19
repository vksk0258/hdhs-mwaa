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
MAX_RETRIES = 5  # 최대 재시도 횟수

with DAG(
    dag_id="hdhs_oracle-to-s3",  # DAG의 고유 식별자
    schedule=None,  # DAG의 예약 일정 없음 (수동 실행)
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=1200),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","검증"]  # DAG에 붙일 태그
) as dag:
    @task(task_id='oracle_to_s3_upload', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def oracle_to_s3_upload():
        """
        Oracle 데이터를 BATCH_SIZE 단위로 읽어오고 CSV로 저장 후 S3에 업로드
        """
        # 임시 디렉토리 생성
        os.makedirs(TMP_DIR, exist_ok=True)

        # 파일 이름 카운터
        chunk_index = 1

        # SQL 실행 (OFFSET-FETCH 또는 ROWNUM 사용)
        offset = 0

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()

        while True:
            query = f"""
                SELECT * FROM (
                    SELECT a.*, ROWNUM rnum FROM (
                        SELECT * FROM {TABLE_NAME}
                    ) a WHERE ROWNUM <= {offset + BATCH_SIZE}
                ) WHERE rnum > {offset}
            """

            df = pd.read_sql(query, oracle_connection)

            if df.empty:
                break

            file_name = f"{TMP_DIR}/LOAD{chunk_index:08d}.csv"  # 파일 이름 형식
            # 헤더 제외하고 저장
            df.to_csv(file_name, index=False, header=False)
            print(f"Chunk {chunk_index} saved to {file_name}")

            # S3 업로드
            s3_path = f"s3://{S3_BUCKET_NAME}/{S3_PREFIX}LOAD{chunk_index:08d}.csv"
            os.system(f"aws s3 cp {file_name} {s3_path}")
            print(f"Uploaded {file_name} to {s3_path}")

            # 로컬 파일 삭제
            os.remove(file_name)
            print(f"Deleted {file_name} from local directory")

            chunk_index += 1

        oracle_connection.close()


    oracle_to_s3_upload=oracle_to_s3_upload()