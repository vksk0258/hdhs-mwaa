from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

table_list = [
    "HDHS_CU.CU_CUST_MST",
    "HDHS_CU.CU_ITNT_CUST_GRD_2_INF",
    "HDHS_CU.CU_ITNT_CUST_GRD_1_INF",
    "HDHS_CU.CU_CUST_INF_AGR_DTL",
    "HDHS_CU.CU_CUST_APP_INF",
    "HDHS_PD.IM_SLITM_MST",
    "HDHS_CM.CM_MD_MST",
    "HDHS_CM.CM_ORGN_MST",
    "HDHS_AM.AM_ALML_MST",
    "HDHS_OD.OD_BASKT_INF",
    "HDHS_CU.CU_SLTD_DTL",
    "HDHS_CU.CU_ONLN_ACSS_LOG",
    "HDHS_BM.BD_MLB_PGM_ALRIM_INF",
    "HDHS_CM.CM_USER_MST",
    "HDHS_DW.CP_CUST_PSN_INF_CHK_MST",
    "HDHS_DW.CP_CUST_SMS_TEL_CHK_MST",
    "HDHS_DW.CP_CUST_DSTN_ADR_CHK_MST",
    "HDHS_DW.AN_PRMO_CUST_DTL",
    "HDHS_OD.OD_ORD_DTL"
]

with DAG(
    dag_id="hdhs_DDL_verifi_daily",  # DAG의 고유 식별자
    schedule=None,  # DAG의 예약 일정 없음 (수동 실행)
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=60),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","검증"]  # DAG에 붙일 태그
) as dag:
    @task(task_id='oracle_col_info_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=15))
    def ora_push(table_list, **kwargs):

        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)
        value_list = []  # 결과 저장 리스트

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        oracle_cursor = oracle_connection.cursor()


        for table in table_list:
            ora_query =f"""
            SELECT COLUMN_NAME
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = UPPER('{table.split('.')[0]}')
              AND TABLE_NAME = UPPER('{table.split('.')[1]}')
            ORDER BY COLUMN_ID
            """
            oracle_cursor.execute(ora_query)
            colums = [row[0] for row in oracle_cursor.fetchall()]
            value_list.append(colums)

        oracle_cursor.close()
        oracle_connection.close()

        ti.xcom_push(key='ora_data', value=value_list)

    @task(task_id='snowflake_col_info_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def snow_push(table_list, **kwargs):

        ti = kwargs['ti']
        value_list = []

        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        for table in table_list:
            snow_query =f"""
            SELECT COLUMN_NAME
            FROM DW_LOAD_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{table.split('.')[0]}'
              AND TABLE_NAME = '{table.split('.')[1]}'
            ORDER BY ORDINAL_POSITION
            """
            snowflake_cursor.execute(snow_query)
            columns = [row[0] for row in snowflake_cursor.fetchall()]
            value_list.append(columns)

        snowflake_cursor.close()
        snowflake_connection.close()

        ti.xcom_push(key='snow_data', value=value_list)


    @task(task_id='insert_comparison_results')
    def insert_result(table_list, **kwargs):
        """
        Oracle과 Snowflake 데이터베이스의 조회 결과를 비교하고, 비교 결과를
        Snowflake의 결과 테이블에 삽입하는 태스크.
        """
        ti = kwargs['ti']  # Task Instance (XCom 접근을 위해 필요)
        snow_db_name = 'DW_LOAD_DB'  # Snowflake 데이터베이스 이름

        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        # 테이블 목록 순회하며 비교 수행
        for idx,table in enumerate(table_list):
            schema_name = table.split('.')[0]  # 테이블의 스키마 이름
            table_name = table.split('.')[1]  # 테이블 이름
            execution_date = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))  # 실행 시간

            # XCom에서 Oracle 및 Snowflake 조회 결과 가져오기
            snow_data = ti.xcom_pull(key='snow_data', task_ids='snowflake_col_info_push_xcom')[idx]
            ora_data = ti.xcom_pull(key='ora_data', task_ids='oracle_col_info_push_xcom')[idx]

            print(ora_data)

            snow_col_cnt = len(snow_data[0])  # Snowflake 레코드 수
            ora_col_cnt = len(ora_data[0])  # Oracle 레코드 수

            is_match = set(snow_data[0]) == set(ora_data[0])

            separator = ", "  # 값들 사이에 넣을 구분자
            snow_only_list = list(set(snow_data[0]) - set(ora_data[0]))
            ora_only_list = list(set(ora_data[0]) - set(snow_data[0]))

            snow_only = separator.join(map(str, snow_only_list))
            ora_only = separator.join(map(str, ora_only_list))

            # 비교 결과를 Snowflake 테이블에 삽입
            insert_query = """
                INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_VERIFY (
                    VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                    IS_MATCH, ORA_COL_CNT, SNOW_COL_CNT, ORA_ONLY, SNOW_ONLY
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            parameters = (
                execution_date,
                snow_db_name,
                schema_name,
                table_name,
                is_match,
                ora_col_cnt,
                snow_col_cnt,
                ora_only,
                snow_only
            )
            snowflake_cursor.execute(insert_query, parameters)

        # 커넥션 닫기
        snowflake_connection.commit()
        snowflake_cursor.close()
        snowflake_connection.close()

    # 태스크 간의 의존성 설정
    ora_push(table_list) >> snow_push(table_list) >> insert_result(table_list)