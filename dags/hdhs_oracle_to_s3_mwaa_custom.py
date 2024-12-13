from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import datetime
from airflow.models import Variable
import os
import pandas as pd

client_path = Variable.get("client_path")
# DAG 기본 설정

dag = DAG(
    dag_id='hdhs_oracle_to_s3_chunked',
    description='Extract Oracle data in chunks, save as CSV to /tmp, upload to S3 one by one, and clean up local files',
    schedule_interval=None,
    dagrun_timeout=datetime.timedelta(minutes=1500),
    tags=['oracle', 's3', 'mwaa'],
)

# 환경 설정
TMP_DIR = "/tmp/oracle_chunks"
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
S3_PREFIX = "dw/HDHS_OD/OD_STLM_INF_CRYPT/"
ORACLE_CONN_ID = "conn_oracle_main"
TABLE_NAME = "OD_STLM_INF_CRYPT"
CHUNK_SIZE = 100000  # 한 번에 처리할 행 개수 (예: 10만 행)


def extract_and_upload_chunks():
    """오라클에서 데이터를 청크 단위로 추출, 로컬 CSV 저장, S3 업로드 후 삭제"""
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID, thick_mode=True, thick_mode_lib_dir=client_path)
    conn = oracle_hook.get_conn()
    cursor = conn.cursor()

    # 테이블의 총 데이터 크기 알아내기
    cursor.execute(f"SELECT COUNT(*) FROM HDHS_OD.{TABLE_NAME}")
    total_rows = cursor.fetchone()[0]
    print(f"Total rows in the table: {total_rows}")

    # 청크 단위로 데이터 추출, 저장, 업로드 및 삭제
    chunk_index = 1
    for offset in range(0, total_rows, CHUNK_SIZE):
        query = f"""
            SELECT * FROM (
                SELECT ROWNUM AS row_num, a.* 
                FROM {TABLE_NAME} a
                WHERE ROWNUM <= {offset + CHUNK_SIZE}
            ) WHERE row_num > {offset}
        """
        df = pd.read_sql(query, conn)
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

    cursor.close()
    conn.close()


# 태스크 정의
extract_and_upload_task = PythonOperator(
    task_id='extract_and_upload_chunks',
    python_callable=extract_and_upload_chunks,
    dag=dag,
)

# DAG 의존성 설정
extract_and_upload_task
