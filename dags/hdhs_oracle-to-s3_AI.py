import pandas as pd
import boto3
import os
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.models import Variable
from airflow.decorators import task
import datetime
import shutil

# 설정 상수
client_path = Variable.get("client_path")
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
S3_PREFIX = "dw/HDHS_OD/OD_STLM_INF_CRYPT/"
ORACLE_CONN_ID = "conn_oracle_main"
TABLE_NAME = "OD_STLM_INF_CRYPT"
BATCH_SIZE = 100000  # 테스트 후 조정
TMP_DIR = "/tmp/oracle_to_s3"

# DAG 정의
with DAG(
        dag_id="hdhs_oracle_to_s3_AI",
        schedule=None,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=1200),
        tags=["현대홈쇼핑", "검증"],
) as dag:
    @task(task_id='oracle_to_s3_upload', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def oracle_to_s3_upload():
        os.makedirs(TMP_DIR, exist_ok=True)
        s3_client = boto3.client('s3')
        chunk_index = 1
        offset = 0

        oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID, thick_mode=True, thick_mode_lib_dir=client_path)

        with oracle_hook.get_conn() as conn:
            while True:
                query = f"""
                    SELECT * FROM (
                        SELECT a.*, ROW_NUMBER() OVER (ORDER BY <primary_key>) AS rnum
                        FROM {TABLE_NAME} a
                    ) WHERE rnum BETWEEN {offset + 1} AND {offset + BATCH_SIZE}
                """
                df = pd.read_sql(query, conn)

                if df.empty:
                    break

                file_name = f"{TMP_DIR}/LOAD{chunk_index:08d}.csv"
                df.to_csv(file_name, index=False)
                print(f"Chunk {chunk_index} saved to {file_name}")

                s3_path = f"{S3_PREFIX}LOAD{chunk_index:08d}.csv"
                s3_client.upload_file(file_name, S3_BUCKET_NAME, s3_path)
                print(f"Uploaded {file_name} to {s3_path}")

                os.remove(file_name)
                print(f"Deleted {file_name}")

                offset += BATCH_SIZE
                chunk_index += 1


    @task(task_id='cleanup_tmp_dir')
    def cleanup_tmp_dir():
        if os.path.exists(TMP_DIR):
            shutil.rmtree(TMP_DIR)
            print(f"Temporary directory {TMP_DIR} cleaned up.")


    oracle_to_s3_upload() >> cleanup_tmp_dir()
