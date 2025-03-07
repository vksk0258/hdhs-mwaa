from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import pandas as pd
import pendulum
import boto3
import json


def make_api_body(msgTitl, Task, start_time, end_time, error_msg):
    api_body = {
        "untdNotfNo": "2024120410500001",
        "untdNotfRcvIdList": [],
        "imdtSendYn": "Y",
        "sendPrrgDtm": "",
        "msgTitl": f"{msgTitl}",
        "txtCntn": f"""
        MWAA 알림 API 테스트입니다
        Task '{Task} failed.
        Start Time: {start_time}
        End Time: {end_time}
        Error: {error_msg}
        """,
        "untdNotfSendWayGbcdList": ["04"],
        "controlYN": "N"
    }
    print("API BODY:", api_body)
    return api_body


with DAG(
        dag_id="dag_MI05_ETL_CHECK_ERROR_01",
        schedule_interval='*/5 * * * *',
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 6, 13, tz="Asia/Seoul"),
        tags=["현대홈쇼핑", "알람", "Scheduled"]

) as dag:
    @task(task_id="alarm_task")
    def alarm_task(**kwargs):
        start_time = kwargs['data_interval_start'].in_tz(pendulum.timezone("Asia/Seoul"))
        end_time = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))

        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')

        select_query = f"""
                        SELECT * FROM DW_ETL_DB.DW_ETC.JOB_RESULT WHERE ENDTIME >= TO_TIMESTAMP_NTZ('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                        AND ENDTIME <= TO_TIMESTAMP_NTZ('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                        ORDER BY ENDTIME ASC
                        """
        print("SELECT QUERY")
        print(select_query)

        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')

        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                result = cur.fetchall()

                # 컬럼 인덱스 찾기
                columns = [desc[0] for desc in cur.description]
                st_index = columns.index("ST")  # 'ST' 컬럼의 인덱스 찾기
                pgmid_index = columns.index("PGMID")  # 프로그램 ID
                starttime_index = columns.index("STARTTIME")  # 시작 시간
                endtime_index = columns.index("ENDTIME")  # 종료 시간
                msg_index = columns.index("MSG")  # 에러 메시지

                for row in result:
                    if row[st_index] == 'ER':
                        # 해당 로우에서 값 가져오기
                        pgmid = row[pgmid_index]
                        starttime = row[starttime_index]
                        endtime = row[endtime_index]
                        msg = row[msg_index]

                        msg = msg.replace("'", "''")

                        MSG_TITL = f"{pgmid} FAILED"
                        TXT_CNTN = f"{pgmid} failed. Start Time: {starttime}, End Time : {endtime}, Error: {msg}"

                        # INSERT 쿼리
                        insert_query = f"""
                                        INSERT INTO HDHS_CM.CM_UNTD_NOTF_EAI_INT
                                        (
                                            UNTD_NOTF_EAI_INT_SEQ, UNTD_NOTF_NO, IMDT_SEND_YN, SEND_PRRG_DTM, 
                                            MSG_TITL, TXT_CNTN, UNTD_NOTF_RCVP_GBCD, RCVP_ID, UNTD_NOTF_SEND_WAY_GBCD, 
                                            RGST_ID, RGST_IP, REG_DTM, CHGP_ID, CHGP_IP, CHG_DTM
                                        )
                                        VALUES
                                        (
                                            00,
                                            '2024120410500001',
                                            NULL, NULL,
                                            '{MSG_TITL}',
                                            '{TXT_CNTN}',
                                            NULL, NULL, NULL, 
                                            NULL, NULL, NULL, 
                                            NULL, NULL, NULL
                                        )
                                    """
                        print("INSERT QUERY")
                        print(insert_query)

                        # INSERT 실행
                        cur.execute(insert_query)
                        conn.commit()

                        # API 호출을 위한 데이터 생성
                        api_body = make_api_body(MSG_TITL, pgmid, start_time, end_time, TXT_CNTN)

                        # API 전송 작업
                        send_notification = SimpleHttpOperator(
                            task_id=f"send_notification_{pgmid}",
                            http_conn_id='conn_hdhs_notify_api',  # Airflow 연결 ID 사용
                            endpoint='/api/dw/cor/v1/untd-notf/insert-send',  # API 엔드포인트
                            method="POST",
                            data=json.dumps(api_body),
                            headers={
                                'Content-Type': 'application/json',
                                'charset': 'UTF-8',
                                'Accept': '*/*'
                            },
                            dag=dag
                        )
                        send_notification.execute(kwargs)

    alarm_task()
