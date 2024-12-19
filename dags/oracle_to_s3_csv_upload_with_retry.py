from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.models import Variable
import os
import subprocess
import time

# 설정 상수
client_path = Variable.get("client_path")
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
S3_PREFIX = "dw/HDHS_OD/OD_STLM_INF_CRYPT/"
ORACLE_CONN_ID = "conn_oracle_main"
TABLE_NAME = "OD_STLM_INF_CRYPT"
BATCH_SIZE = 1000000  # 한 파일당 저장할 행 수
TMP_DIR = "/tmp/oracle_to_s3"
MAX_RETRIES = 5  # 최대 재시도 횟수

dag = DAG(
    dag_id='oracle_to_s3_csv_upload_with_retry',
    schedule_interval=None,  # 수동 실행
    catchup=False,
    tags=['oracle', 's3', 'csv_upload'],
)

def extract_and_upload_to_s3(**kwargs):
    """
    Oracle 데이터를 BATCH_SIZE 단위로 읽어오고 CSV로 저장 후 S3에 업로드
    """
    # 임시 디렉토리 생성
    os.makedirs(TMP_DIR, exist_ok=True)

    # 파일 이름 카운터
    file_counter = 1

    # SQL 실행 (OFFSET-FETCH 또는 ROWNUM 사용)
    offset = 0

    # Oracle Hook 생성 및 연결 유지
    oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID, thick_mode=True, thick_mode_lib_dir=client_path)

    try:
        while True:
            retries = 0
            while retries < MAX_RETRIES:
                try:
                    query = f"""
                        SELECT * FROM (
                            SELECT a.*, ROWNUM rnum FROM (
                                SELECT * FROM {TABLE_NAME}
                            ) a WHERE ROWNUM <= {offset + BATCH_SIZE}
                        ) WHERE rnum > {offset}
                    """

                    # 데이터 읽기
                    records = oracle_hook.get_pandas_df(sql=query)

                    # 데이터가 없으면 종료
                    if records.empty:
                        return

                    # CSV 파일로 저장
                    file_name = f"LOAD{file_counter:08}.csv"
                    local_file_path = os.path.join(TMP_DIR, file_name)
                    records.to_csv(local_file_path, index=False)

                    # S3로 업로드
                    s3_key = os.path.join(S3_PREFIX, file_name)
                    subprocess.run(["aws", "s3", "cp", local_file_path, f"s3://{S3_BUCKET_NAME}/{s3_key}"])

                    # 로그 출력
                    print(f"Uploaded {file_name} to s3://{S3_BUCKET_NAME}/{s3_key}")

                    # 로컬 파일 삭제
                    os.remove(local_file_path)

                    # 다음 배치 준비
                    offset += BATCH_SIZE
                    file_counter += 1

                    # 성공하면 재시도 루프 탈출
                    break

                except Exception as e:
                    retries += 1
                    print(f"Error occurred: {e}. Retrying {retries}/{MAX_RETRIES}...")
                    time.sleep(10)  # 재시도 전에 대기

                    if retries == MAX_RETRIES:
                        print(f"Max retries reached. Aborting at offset {offset}.")
                        raise
    finally:
        # 연결 종료
        oracle_hook.get_conn().close()
        print("Oracle connection closed.")

extract_upload_task = PythonOperator(
    task_id='extract_and_upload_to_s3',
    python_callable=extract_and_upload_to_s3,
    provide_context=True,
    dag=dag,
)

extract_upload_task
