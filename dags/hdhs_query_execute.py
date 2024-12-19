from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
from datetime import datetime

# 환경 변수 설정
client_path = Variable.get("client_path")
sql_query = Variable.get("query")


def oracle_conn_main_test(**kwargs):
    """
    Oracle DB에서 쿼리를 수행하고 결과를 XCom에 저장
    """
    oracle_hook = OracleHook(
        oracle_conn_id='conn_oracle_main',
        thick_mode=True,
        thick_mode_lib_dir=client_path
    )

    # Oracle DB 연결 및 쿼리 실행
    with oracle_hook.get_conn() as connection:
        cursor = connection.cursor()
        cursor.execute(sql_query)
        data = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # XCom에 컬럼 이름과 데이터 반환
        return {"columns": column_names, "data": data}


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
