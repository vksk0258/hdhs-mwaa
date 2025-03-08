from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
import requests
import json



def make_api_body(dag_id, task_id, start_date, end_date, error_logs):
    api_body = {
        "untdNotfNo": "2024120410500001",
        "untdNotfRcvIdList": [],
        "imdtSendYn": "Y",
        "sendPrrgDtm": "",
        "msgTitl": f"{task_id} failed",
        "txtCntn": f"""MWAA 알림 API 테스트입니다
        DAG ID: {dag_id}
        Task ID: {task_id}
        Start Time: {start_date}
        End Time: {end_date}
        Error: {error_logs}""",
        "untdNotfSendWayGbcdList": ["04"],
        "controlYN": "N"
    }
    print("API BODY:", api_body)
    return api_body

def notify_api_on_error(context):
    task_instance = context.get('task_instance')
    dag_id = context.get('dag').dag_id

    task_id = task_instance.task_id

    start_date = task_instance.start_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date = task_instance.end_date .strftime('%Y-%m-%d %H:%M:%S')

    error_logs = task_instance.error


    # API 호출을 위한 데이터 생성
    api_body = make_api_body(dag_id, task_id, start_date, end_date, error_logs)

    url = "https://boapi.hmall.com/api/dw/cor/v1/untd-notf/insert-send"  # 예제 API

    headers = {'Content-Type': 'application/json',
                'charset': 'UTF-8',
                'Accept': '*/*'}

    response = requests.post(url,
                             json=api_body,
                             headers=headers)

    if response.status_code == 200:  # 200: 리소스 생성 성공
        print("✅ POST 성공:", response.json())
    else:
        print("❌ POST 실패:", response.status_code)


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
        provide_context=True,  # XCom 사용을 위해 추가
        on_failure_callback=notify_api_on_error,
    )

    oracle_conn_test_task
