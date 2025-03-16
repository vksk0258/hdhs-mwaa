from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.models import Variable  # Airflow 변수 관리
import requests
import pendulum

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

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
        dag_id="dag_MI05_ETL_CHECK_ERROR_01",
        schedule_interval='*/10 * * * *',
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
                        SELECT {', '.join(columns)} FROM S_DWETL_DW_API_DB.DW_ETC.JOB_RESULT 
                        WHERE ENDTIME >= TO_TIMESTAMP_NTZ('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                        AND ENDTIME <= TO_TIMESTAMP_NTZ('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                        ORDER BY ENDTIME ASC
                        """
        print("SELECT QUERY")
        print(select_query)

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_api')

        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_query)
                result = cur.fetchall()
                # wh_suspend_query = "ALTER WAREHOUSE DW_ETL_ERR_CHK_WH SUSPEND"
                # print(wh_suspend_query)
                # cur.execute(wh_suspend_query)

        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cur:

                for latest_row in result:

                    msg = latest_row[10] if latest_row[10] is None else latest_row[10].replace("'", "''")[-100:]

                    rst_insert_query = f"""
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
                        api_body = make_api_body(MSG_TITL, pgmid, start_time, end_time, TXT_CNTN)

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


    alarm_task(columns,ora_columns)
