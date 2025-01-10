from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pendulum


# Snowflake 쿼리를 실행하고 배치 시간 내 데이터를 확인
def check_endtime_within_batch(**kwargs):
    # 실행 시간 정보를 가져오기
    data_interval_start = kwargs['data_interval_start'].in_tz(pendulum.timezone("Asia/Seoul"))
    data_interval_end = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul")) + timedelta(hours=1)
    # Snowflake 연결
    snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snowflake_etl")

    # SQL 쿼리: 배치 시간 동안 ENDTIME 안의 데이터를 확인
    sql_query = f"""
    SELECT ST, ENDTIME
    FROM DW_ETL_DB.CONFIG.JOB_RESULT
    WHERE ENDTIME >= '{data_interval_start}' AND ENDTIME < '{data_interval_end}' AND ST = 'ER';
    """

    # Snowflake에서 쿼리 실행
    result = snowflake_hook.get_records(sql_query)

    if result:
        print(f"Found ER records within batch time: {result}")
    else:
        print("No ER records found within batch time.")

with DAG(
        dag_id='hdhs_task_error_check_5m',
        description='Check ENDTIME within batch window and ST value in Snowflake table',
        schedule_interval='0 * * * *',  # 5분마다 실행
        start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['snowflake', 'ENDTIME_check'],
) as dag:
    check_endtime_task = PythonOperator(
        task_id='check_endtime_within_batch_task',
        python_callable=check_endtime_within_batch,
        provide_context=True,  # **kwargs 사용을 위해 필요
    )

    check_endtime_task
