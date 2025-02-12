from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")
KST = pendulum.timezone("Asia/Seoul")
execution_time = pendulum.now(KST)

# DAG 정의
with DAG(
        dag_id="hdhs_procedure_verifi_daily",  # DAG의 고유 식별자
        start_date=pendulum.datetime(2025, 2, 10, tz="Asia/Seoul"),  # DAG 시작 날짜 및 타임존 설정
        schedule_interval="0 11 * * *",  # 매일 00시에 실행
        catchup=False,  # 과거 데이터 실행을 스킵
        dagrun_timeout=datetime.timedelta(minutes=500),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "검증"]  # DAG에 붙일 태그
) as dag:
    @task(task_id='procedure_verification', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def procedure_verification(**kwargs):
        start_time = execution_time.format("YYYY-MM-DD 00:10:00")

        # Snowflake와 Oracle의 데이터베이스 연결 설정
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_OCI', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        oracle_cursor = oracle_connection.cursor()

        # Snowflake에서 최근 실행된 프로시저 목록 조회
        procedure_list_query = f"""
                    SELECT PGMID, JBPMT, STARTTIME
                    FROM DW_ETL_DB.DW_ETC.JOB_RESULT 
                    WHERE STARTTIME >= TO_DATE('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    """

        snowflake_cursor.execute(procedure_list_query)
        procedure_list = [(row[0], row[1], row[2].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]) for row in snowflake_cursor.fetchall()]

        print(procedure_list)

        for procedure in procedure_list:
            try:
                # Snowflake와 Oracle에서 해당 프로시저의 실행 결과 조회
                snow_query = f"""
                        SELECT PGMID, STARTTIME, ENDTIME, ST, JBPMT, READCNT, BYCNT, ERRCNT, UPDCNT, WRTCNT FROM DW_ETL_DB.DW_ETC.JOB_RESULT 
                        WHERE PGMID = '{procedure[0]}' 
                        AND JBPMT = '{procedure[1]}'
                        AND STARTTIME = '{procedure[2]}'
                        ORDER BY STARTTIME ASC
                        """

                ora_query = f"""
                        SELECT PGMID, STARTTIME, ENDTIME, ST, JBPMT, READCNT, BYCNT, ERRCNT, UPDCNT, WRTCNT FROM DW_ETC.JOB_RESULT 
                        WHERE PGMID = '{procedure[0]}' 
                        AND JBPMT = '{procedure[1]}'
                        AND STARTTIME >= TO_DATE('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                        ORDER BY STARTTIME ASC
                        """

                print(snow_query)
                print(ora_query)

                snow_results = snowflake_cursor.execute(snow_query).fetchall()
                ora_results = oracle_cursor.execute(ora_query).fetchall()

                flag = None  # 기본값 설정

                if not snow_results:
                    flag = "snowflake"
                elif not ora_results:
                    flag = "oracle"

                if not snow_results or not ora_results:
                    raise ValueError(f"No data found for DB: [{flag}] PGMID: {procedure[0]}, JBPMT: {procedure[1]}")

                print(f"Snowflake Query Result: {snow_results}")
                print(f"Oracle Query Result: {ora_results}")

                for snow_result, ora_result in zip(snow_results, ora_results):

                    # 실행 결과 비교 및 차이 계산
                    SMR_DT = execution_time.format("YYYY-MM-DD")
                    PGMID = snow_result[0]
                    ORA_START_TIME = ora_result[1].strftime('%Y-%m-%d %H:%M:%S') if snow_result[1] else None
                    ORA_END_TIME = "'"+ora_result[2].strftime('%Y-%m-%d %H:%M:%S')+"'" if ora_result[2] else 'Null'
                    ORA_EXEC_TIME = ((ora_result[2] or datetime.datetime.min) - (
                                ora_result[1] or datetime.datetime.min)).total_seconds()
                    ORA_ST = ora_result[3]
                    SNOW_START_TIME = snow_result[1].strftime('%Y-%m-%d %H:%M:%S') if snow_result[1] else None
                    SNOW_END_TIME = "'"+snow_result[2].strftime('%Y-%m-%d %H:%M:%S')+"'" if snow_result[2] else 'Null'
                    SNOW_EXEC_TIME = ((snow_result[2] or datetime.datetime.min) - (
                                snow_result[1] or datetime.datetime.min)).total_seconds()
                    SNOW_ST = snow_result[3]
                    ORA_SNOW_EXEC_TIME = ORA_EXEC_TIME - SNOW_EXEC_TIME
                    JBPMT = snow_result[4]
                    READ_CNT_DIFF = (ora_result[5] or 0) - (snow_result[5] or 0)
                    BYCNT_DIFF = (ora_result[6] or 0) - (snow_result[6] or 0)
                    ERRCNT_DIFF = (ora_result[7] or 0) - (snow_result[7] or 0)
                    UPDCNT_DIFF = (ora_result[8] or 0) - (snow_result[8] or 0)
                    WRTCNT_DIFF = (ora_result[9] or 0) - (snow_result[9] or 0)
                    READ_CNT_ORA = ora_result[5] or 0
                    BYCNT_ORA = ora_result[6] or 0
                    ERRCNT_ORA = ora_result[7] or 0
                    UPDCNT_ORA = ora_result[8] or 0
                    WRTCNT_ORA = ora_result[9] or 0
                    MATCH_YN = 'Y' if READ_CNT_DIFF == BYCNT_DIFF == ERRCNT_DIFF == UPDCNT_DIFF == WRTCNT_DIFF else 'N'

                    # 비교 결과를 Snowflake 테이블에 저장
                    isrt_query = f"""
                                INSERT INTO DW_ETL_DB.DW_ETC.ORA_SNOW_DATA_PIPE_VAL
                                VALUES ('{SMR_DT}',
                                        '{PGMID}',
                                        '{ORA_START_TIME}',
                                        {ORA_END_TIME},
                                        {ORA_EXEC_TIME:.2f}, 
                                        '{ORA_ST}', 
                                        '{SNOW_START_TIME}',
                                        {SNOW_END_TIME},
                                        {SNOW_EXEC_TIME:.2f}, 
                                        '{SNOW_ST}',
                                        {ORA_SNOW_EXEC_TIME:.2f}, 
                                        '{JBPMT}', 
                                        {READ_CNT_DIFF}, 
                                        {BYCNT_DIFF}, 
                                        {ERRCNT_DIFF}, 
                                        {UPDCNT_DIFF}, 
                                        {WRTCNT_DIFF},
                                        {READ_CNT_ORA}, 
                                        {BYCNT_ORA}, 
                                        {ERRCNT_ORA}, 
                                        {UPDCNT_ORA}, 
                                        {WRTCNT_ORA}, 
                                        '{MATCH_YN}'
                                        ,NULL)
                                """
                    snowflake_cursor.execute(isrt_query)
            except Exception as e:
                # 오류 발생 시 에러 정보 저장
                error_query = f"""
                        INSERT INTO DW_ETL_DB.DW_ETC.ORA_SNOW_DATA_PIPE_VAL (SMR_DT,PGMID,ORA_ST,JBPMT,SNOW_START_TIME, ERR_MSG)
                        VALUES ('{execution_time.format("YYYY-MM-DD")}', '{procedure[0]}','XX','{procedure[1]}','{procedure[2]}', '{str(e)}')
                        """
                snowflake_cursor.execute(error_query)
                print(f"Error processing {procedure[0]}: {e}")

        # 커넥션 닫기
        snowflake_connection.commit()
        snowflake_cursor.close()
        snowflake_connection.close()

        oracle_cursor.close()
        oracle_connection.close()


    # 태스크 실행
    procedure_verification()
