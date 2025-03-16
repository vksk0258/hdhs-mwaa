from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta
from common.notify_error_functions import notify_api_on_error
from airflow.models import Variable  # Airflow 변수 관리
import pandas as pd
import pendulum
import boto3
import json

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_1200_CP_SMR_MST_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

def make_api_body(msgTitl, pgmid, start_time, end_time, error_msg):
    api_body = {
        "untdNotfNo": "2024120410500001",
        "untdNotfRcvIdList": [],
        "imdtSendYn": "Y",
        "sendPrrgDtm": "",
        "msgTitl": f"{msgTitl}",
        "txtCntn": f"""
        Snowflake 배치 프로시저 장애 알람
        [PGMID]: {pgmid}
        [Start Time]: {start_time}
        [End Time]: {end_time}
        [Error]: {error_msg}
        """,
        "untdNotfSendWayGbcdList": ["04"],
        "controlYN": "Y",
        "controlGbcd": "dw"
    }
    print("API BODY:", api_body)
    return api_body

def execute_procedure_insu(procedure_name, p_start, p_end,conn_id,**kwargs):
    """
    Executes a stored procedure in Snowflake and logs the result.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{p_start}', '{p_end}')"
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                # Handle empty result properly
                result_message = result[0][0] if result and result[0] else "No result returned"
                print(f"Procedure result: {result_message}")

                select_query = f"""
                                        SELECT * FROM INSU_DB.DW_ETC.JOB_RESULT
                                        WHERE PGMID = '{procedure_name}'
                                        ORDER BY ENDTIME DESC
                                        LIMIT 1
                                        """
                cur.execute(select_query)
                latest_row = cur.fetchone()

            oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
            with oracle_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    msg = latest_row[10] if latest_row[10] is None else latest_row[10].replace("'", "''")[-100:]

                    rst_insert_query = f"""
                    INSERT INTO HDHS_DW.DW_EXEC_RST
                    VALUES(
                    '{latest_row[0]}',
                    TO_TIMESTAMP('{latest_row[1]}','YYYY-MM-DD HH24:MI:SS'),
                    TO_TIMESTAMP('{latest_row[2]}','YYYY-MM-DD HH24:MI:SS'),
                    '{latest_row[3]}',
                    '{latest_row[4]}',
                    {latest_row[5]},
                    {latest_row[6]},
                    {latest_row[7]},
                    {latest_row[8]},
                    {latest_row[9] or 0},
                    '{msg}'
                    )
                    """
                    print("RESULT INSERT QUERY")
                    print(rst_insert_query)

                    cur.execute(rst_insert_query)
                    conn.commit()

                    if latest_row[3] == 'ER':
                        pgmid = latest_row[0]
                        starttime = latest_row[1]
                        endtime = latest_row[2]

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

                        # API 호출을 위한 데이터 생성
                        api_body = make_api_body(MSG_TITL, pgmid, starttime, endtime, TXT_CNTN)

                        # API 전송 작업
                        send_notification = SimpleHttpOperator(
                            task_id="send_notification_" + pgmid,
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

    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")

# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_VLID_TERM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0010_VLID_TERM_01","MART프로시져"]
) as dag:
    task_SP_BCU_CUST_STAT_MST = PythonOperator(
        task_id="task_SP_BCU_CUST_STAT_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_STAT_MST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL = PythonOperator(
        task_id="task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_VLID_TERM_EMAIL_DTL", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_DWCU_CUST_SMS_DROP = PythonOperator(
        task_id="task_SP_DWCU_CUST_SMS_DROP",
        python_callable=execute_procedure,
        op_args=["SP_DWCU_CUST_SMS_DROP", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_DWCU_CUST_INSU_DROP = PythonOperator(
        task_id="task_SP_DWCU_CUST_INSU_DROP",
        python_callable=execute_procedure_insu,
        op_args=["SP_DWCU_CUST_INSU_DROP", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_CUST_STAT_MST >> \
    task_SP_BMK_CUST_VLID_TERM_EMAIL_DTL >> \
    task_SP_DWCU_CUST_SMS_DROP >> \
    task_SP_DWCU_CUST_INSU_DROP





