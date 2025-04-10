from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import os
import requests
import json



# 환경 변수 설정
client_path = Variable.get("client_path")
sql_query = Variable.get("query")
conn = Variable.get("conn")


def oracle_conn_main_test(**kwargs):
    """
    Snowflake DB에서 쿼리를 수행하고 결과를 XCom에 저장
    """
    oracle_hook = OracleHook(oracle_conn_id=conn, thick_mode=True, thick_mode_lib_dir=client_path)

    # Oracle DB 연결 및 쿼리 실행
    with oracle_hook.get_conn() as connection:
        with connection.cursor() as cursor:

            cursor.execute(sql_query)
            rows = cursor.fetchall()

            for row in rows:
                print(row)

def snow_conn_main_test(**kwargs):
    """
    Snowflake DB에서 쿼리를 수행하고 결과를 XCom에 저장
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn)

    # Oracle DB 연결 및 쿼리 실행
    with snowflake_hook.get_conn() as connection:
        with connection.cursor() as cursor:

            cursor.execute(sql_query)
            rows = cursor.fetchall()

            for row in rows:
                print(row)

# DAG 정의
with DAG(
        dag_id="hdhs_query_execute2",
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

    # PythonOperator 태스크 정의
    snow_conn_main_test = PythonOperator(
        task_id='snow_conn_main_test',
        python_callable=snow_conn_main_test,
        provide_context=True  # XCom 사용을 위해 추가

    )

    [oracle_conn_test_task,snow_conn_main_test]
