from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
from datetime import datetime

# 환경 변수 설정
client_path = Variable.get("client_path")
sql_query = Variable.get("query")
conn = Variable.get("conn")

TMP_DIR = "/tmp/ods"
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"


def oracle_conn_main_test(**kwargs):
    """
    Oracle DB에서 쿼리를 수행하고 결과를 XCom에 저장
    """
    oracle_hook = OracleHook(
        oracle_conn_id=conn,
        thick_mode=True,
        thick_mode_lib_dir=client_path
    )
    TMP_DIR="/tmp"
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    # Oracle DB 연결 및 쿼리 실행
    with oracle_hook.get_conn() as connection:
        df = pd.read_sql(sql_query, connection)
        df.to_csv("/tmp/tmp.csv", index=True, header=True)
        print(df)
        os.system(f"aws s3 cp /tmp/tmp.csv s3://hdhs-dw-mwaa-s3/tmp/tmp.csv")


# DAG 정의
with DAG(
        dag_id="hdhs_query_execute",
        schedule_interval=None,
        catchup=False,
        tags=["현대홈쇼핑"]
) as dag:
    # PythonOperator 태스크 정의
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test,
        provide_context=True  # XCom 사용을 위해 추가
    )

    oracle_conn_test_task
