from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pandas as pd
import pprint
import time
import csv
import datetime


client_path = Variable.get("client_path")
query = Variable.get("query")
def oracle_conn_main_test():
    # OracleHook을 사용하여 Airflow Connection으로 연결
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main',thick_mode=True,thick_mode_lib_dir=client_path)  # Airflow UI에서 설정한 Connection ID
    sql_query = query  # 실행할 SQL 쿼리
    output_path = '/tmp/output2.csv'  # 저장할 CSV 경로

    # 연결을 열고 데이터 쓰기
    with oracle_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # CSV 파일에 데이터 쓰기
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)

            # 컬럼 헤더 쓰기
            headers = [col[0] for col in cursor.description]
            writer.writerow(headers)

            # 데이터를 한 줄씩 읽어 쓰기
            for row in cursor:
                writer.writerow(row)

        print(f"CSV 파일이 저장되었습니다: {output_path}")

with DAG(
    dag_id="hdhs_conn_test_main_thick",
    schedule_interval=None,
    dagrun_timeout=datetime.timedelta(minutes=300),
    tags=["현대홈쇼핑"]
) as dag:
    var_value = Variable.get("sample_key")
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        # 스케쥴러의 부하를 줄이기위해 템플릿 문법으로 전역변수를 가져오는 것을 추천함
        bash_command=var_value
    )

    oracle_conn_test_task >> bash_var_2