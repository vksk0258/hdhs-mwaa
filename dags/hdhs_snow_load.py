from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import datetime
import pprint
import time
def load_data():
    point4 = time.time()
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("PUT file:///tmp/output2.csv @MWAA_STAGE")

    point5 = time.time()
    pprint.pprint(f"PUT을 활용해서 CSV파일을 스테이지에 업로드 하는 시간: {point5 - point4} sec")
    # 데이터 로드
    cursor.execute("""
            COPY INTO OD_HPNT_PAY_APRVL_DTL_CRYPT
            FROM @MWAA_STAGE/output2.csv
            FILE_FORMAT = (TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY='"',  SKIP_HEADER = 1)
        """)

    connection.close()
    cursor.close()

    point6 = time.time()
    pprint.pprint(f"COPY INTO 명령어를 날려 CSV파일을 테이블로 저장하는 시간: {point6 - point5} sec")

with DAG(
    dag_id="hdhs_snow_load",
    schedule_interval=None,
    dagrun_timeout=datetime.timedelta(minutes=300),
    tags=["현대홈쇼핑"]
) as dag:
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )

    load_data_task