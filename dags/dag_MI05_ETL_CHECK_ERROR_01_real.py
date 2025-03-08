from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable  # Airflow 변수 관리
import pandas as pd
import pendulum
import boto3
import json

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

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

columns = [
                "PGMID", "STARTTIME", "ENDTIME", "ST", "JBPMT",
                "READCNT", "BYCNT", "ERRCNT", "UPDCNT", "WRTCNT",
                "MSG"
            ]

ora_columns = [
                "PRG_NM", "STRT_DTM", "END_DTM", "EXEC_RST_VAL", "PARA_VAL",
                "EXEC_CNT", "SKIP_CNT", "ERR_CNT", "UPDT_CNT", "WRT_CNT",
                "MSG"
            ]


with DAG(
        dag_id="dag_MI05_ETL_CHECK_ERROR_01_real",
        schedule_interval='*/5 * * * *',
        catchup=False,
        start_date=pendulum.datetime(2025, 3, 7, 18,0, tz="Asia/Seoul"),
        tags=["현대홈쇼핑", "알람", "Scheduled"]

) as dag:
    @task(task_id="alarm_task")
    def alarm_task(columns,ora_columns,**kwargs):
        start_time = kwargs['data_interval_start'].in_tz(pendulum.timezone("Asia/Seoul"))
        end_time = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))

        start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')

        select_query = f"""
                        SELECT {', '.join(columns)} FROM DW_ETL_DB.DW_ETC.JOB_RESULT 
                        WHERE ENDTIME >= TO_TIMESTAMP_NTZ('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                        AND ENDTIME <= TO_TIMESTAMP_NTZ('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                        ORDER BY ENDTIME ASC
                        """
        print("SELECT QUERY")
        print(select_query)

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')

        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                result = cur.fetchall()

        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for latest_row in result:
                    if latest_row[3] == 'ER':

                        msg = latest_row[10] if latest_row[10] is None else latest_row[10].replace("'", "''")
                        msg = msg[-199:]

                        # INSERT 쿼리
                        insert_query = f"""
                                            INSERT INTO HDHS_DW.DW_EXEC_RST ({", ".join(ora_columns)})
                                            VALUES (
                                            '{latest_row[0]}',
                                            TO_TIMESTAMP('{latest_row[1]}','YYYY-MM-DD HH24:MI:SS'),
                                            TO_TIMESTAMP('{latest_row[2]}','YYYY-MM-DD HH24:MI:SS'),
                                            '{latest_row[3]}',
                                            '{latest_row[4]}',
                                            {latest_row[5]},
                                            {latest_row[6]},
                                            {latest_row[7]},
                                            {latest_row[8]},
                                            {latest_row[9]},
                                            '{msg}'
                                            )
                                        """
                        print("INSERT QUERY")
                        print(insert_query)

                        cur.execute(insert_query)
                        conn.commit()

                        pgmid = {latest_row[0]}
                        starttime = {latest_row[1]}
                        endtime = {latest_row[2]}

                        MSG_TITL = f"{pgmid} FAILED"
                        TXT_CNTN = f"{pgmid} failed. Start Time: {starttime}, End Time : {endtime}, Error: {msg}"

                        # API 호출을 위한 데이터 생성
                        api_body = make_api_body(MSG_TITL, {pgmid}, start_time, end_time, TXT_CNTN)

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

    alarm_task(columns,ora_columns)
