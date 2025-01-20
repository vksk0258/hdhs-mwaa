from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
import pendulum
import json

# Snowflake 쿼리를 실행하고 배치 시간 내 데이터를 확인하고 데이터를 context에 저장
def check_endtime_within_batch(**kwargs):
    # 실행 시간 정보를 가져오기
    data_interval_start = kwargs['data_interval_start'].in_tz(pendulum.timezone("Asia/Seoul"))
    data_interval_end = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))

    # Snowflake 연결
    snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snowflake_etl")

    # SQL 쿼리: 배치 시간 동안 ENDTIME 안의 데이터를 확인
    sql_query = f"""
    SELECT PGMID, STARTTIME, ENDTIME, MSG
    FROM DW_ETL_DB.DW_ETC.JOB_RESULT
    WHERE ENDTIME >= '{data_interval_start}' AND ENDTIME < '{data_interval_end}' AND ST = 'ER';
    """

    # Snowflake에서 쿼리 실행
    result = snowflake_hook.get_records(sql_query)

    # 데이터를 다음 태스크에 전달하기 위해 XCom에 저장
    if result:
        task_failures = [
            {
                "task_name": row[0],
                "start_time": row[1].strftime('%Y-%m-%d %H:%M:%S'),
                "end_time": row[2].strftime('%Y-%m-%d %H:%M:%S'),
                "error_message": row[3]
            } for row in result
        ]
        kwargs['ti'].xcom_push(key='task_failures', value=task_failures)
        print(f"Found ER records within batch time: {task_failures}")
    else:
        print("No ER records found within batch time.")

# API 호출 시 필요한 데이터를 동적으로 생성
def send_error_info_body(**kwargs):
    # XCom에서 데이터를 가져옴
    task_failures = kwargs['ti'].xcom_pull(key='task_failures', task_ids='check_endtime_within_batch_task')

    if task_failures:
        for failure in task_failures:
            api_body = {
                "untdNotfNo": "2024120410500001",
                "untdNotfRcvIdList": [],
                "imdtSendYn": "Y",
                "sendPrrgDtm": "",
                "msgTitl": f"Task Failure Alert: {failure['task_name']}",
                "txtCntn": f"""
                MWAA 알림 API 테스트입니다
                Task '{failure['task_name']}' failed.
                Start Time: {failure['start_time']}
                End Time: {failure['end_time']}
                Error: {failure['error_message']}
                """,
                "untdNotfSendWayGbcdList": ["04"]
            }

            # JSON 문자열로 변환하여 저장
            api_body_str = json.dumps(api_body, ensure_ascii=False)  # ensure_ascii=False로 UTF-8 유지
            kwargs['ti'].xcom_push(key='api_body', value=api_body_str)
    print(api_body)

with DAG(
        dag_id='hdhs_task_error_check_5m',
        description='Check ENDTIME within batch window and ST value in Snowflake table',
        schedule_interval='*/5 * * * *',  # 매 5분마다 실행
        start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
        catchup=False,
        tags=['snowflake', 'ENDTIME_check'],
) as dag:

    check_endtime_task = PythonOperator(
        task_id='check_endtime_within_batch_task',
        python_callable=check_endtime_within_batch,
        provide_context=True,  # **kwargs 사용을 위해 필요
    )

    send_error_info_task = PythonOperator(
        task_id='send_error_info_task',
        python_callable=send_error_info_body,
        provide_context=True,
    )

    send_error_info = SimpleHttpOperator(
        task_id='send_error_info',
        http_conn_id='conn_hdhs_notify_api',
        endpoint='/api/co/cor/v1/untd-notf/insert-send',
        method='POST',
        headers={
            'Content-Type': 'application/json',
            'charset': 'UTF-8',
            'Accept': '*/*'
        },
        data="{{ ti.xcom_pull(task_ids='send_error_info_task', key='api_body') }}",  # XCom에서 JSON 문자열 가져옴
        log_response=True,
    )

    check_endtime_task >> send_error_info_task >> send_error_info

