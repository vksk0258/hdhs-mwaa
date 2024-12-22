from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터

# Oracle Client 라이브러리 경로를 Airflow 변수에서 가져옴
client_path = Variable.get("client_path")

# 검증 대상 테이블 목록
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

# DAG 정의
with DAG(
        dag_id="hdhs_DDL_verifi_daily",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=60),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "검증"]  # DAG에 붙일 태그
) as dag:
    # Oracle 테이블의 컬럼 정보를 가져오는 태스크
    @task(task_id='oracle_col_info_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=15))
    def ora_push(table_list, **kwargs):
        """
        Oracle 데이터베이스에서 테이블 컬럼 정보를 조회하고
        XCom을 통해 다음 태스크로 전달
        """
        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)
        value_list = []  # 결과 저장 리스트

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        oracle_cursor = oracle_connection.cursor()

        # 테이블 목록 순회하며 컬럼 정보 조회
        for table in table_list:
            ora_query = f"""
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

        # 조회 결과를 XCom으로 푸시
        ti.xcom_push(key='ora_data', value=value_list)


    # Snowflake 테이블의 컬럼 정보를 가져오는 태스크
    @task(task_id='snowflake_col_info_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def snow_push(table_list, **kwargs):
        """
        Snowflake 데이터베이스에서 TEMP, OG, PI 컬럼 정보를 조회하고
        XCom을 통해 다음 태스크로 전달
        """
        ti = kwargs['ti']
        temp_value_list = []
        og_value_list = []
        pi_value_list = []

        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        # 테이블 목록 순회하며 TEMP, OG, PI 컬럼 정보 조회
        for table in table_list:
            temp_snow_query = f"""
            SELECT COLUMN_NAME
            FROM DW_LOAD_DB.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'TEMP'
              AND TABLE_NAME = 'TEMP_{table.split('.')[1]}'
            ORDER BY ORDINAL_POSITION
            """

            og_snow_query = f"""
            SELECT COLUMN_NAME
              FROM DW_LOAD_DB.INFORMATION_SCHEMA.COLUMNS
             WHERE TABLE_SCHEMA = '{table.split('.')[0]}'
               AND TABLE_NAME = '{table.split('.')[1]}'
            ORDER BY ORDINAL_POSITION
            """

            pi_snow_query = f"""
            SELECT COLUMN_NAME
              FROM DW_LOAD_DB.CONFIG.TB_PI_LIST
             WHERE PI_YN = 'Y'
               AND TABLE_SCHEMA = '{table.split('.')[0]}'
               AND TABLE_NAME = '{table.split('.')[1]}';
            """

            # TEMP 컬럼 조회 및 개인정보 컬럼 제외
            snowflake_cursor.execute(temp_snow_query)
            temp_columns = [row[0] for row in snowflake_cursor.fetchall()]
            temp_columns = list(set(temp_columns) - set(['GBN', 'DMS_SCN']))
            temp_value_list.append(temp_columns)

            # OG 컬럼 조회
            snowflake_cursor.execute(og_snow_query)
            og_columns = [row[0] for row in snowflake_cursor.fetchall()]
            og_value_list.append(og_columns)

            # PI 컬럼 조회
            snowflake_cursor.execute(pi_snow_query)
            pi_columns = [row[0] for row in snowflake_cursor.fetchall()]
            pi_value_list.append(pi_columns)

        snowflake_cursor.close()
        snowflake_connection.close()

        # 조회 결과를 XCom으로 푸시
        ti.xcom_push(key='temp_snow_data', value=temp_value_list)
        ti.xcom_push(key='og_snow_data', value=og_value_list)
        ti.xcom_push(key='pi_snow_data', value=pi_value_list)


    # Oracle과 Snowflake 데이터를 비교하고 결과를 삽입하는 태스크
    @task(task_id='insert_comparison_results')
    def insert_result(table_list, **kwargs):
        """
        Oracle과 Snowflake 데이터베이스의 조회 결과를 비교하고,
        비교 결과를 Snowflake 결과 테이블에 삽입
        """
        ti = kwargs['ti']  # Task Instance (XCom 접근을 위해 필요)
        snow_db_name = 'DW_LOAD_DB'  # Snowflake 데이터베이스 이름

        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        # 테이블 목록 순회하며 데이터 비교 및 결과 삽입
        for idx, table in enumerate(table_list):
            schema_name = table.split('.')[0]  # 테이블의 스키마 이름
            table_name = table.split('.')[1]  # 테이블 이름
            execution_date = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))  # 실행 시간

            # XCom에서 데이터 가져오기
            temp_snow_data = ti.xcom_pull(key='temp_snow_data', task_ids='snowflake_col_info_push_xcom')[idx]
            og_snow_data = ti.xcom_pull(key='og_snow_data', task_ids='snowflake_col_info_push_xcom')[idx]
            pi_snow_data = ti.xcom_pull(key='pi_snow_data', task_ids='snowflake_col_info_push_xcom')[idx]
            ora_data = ti.xcom_pull(key='ora_data', task_ids='oracle_col_info_push_xcom')[idx]

            print(f"오라클 {table_name} : {ora_data}")
            print(f"스노우 {table_name} : {temp_snow_data}")
            print("=============================================")

            # 비교 결과 계산
            snow_col_cnt = len(temp_snow_data)
            og_col_cnt = len(og_snow_data)
            pi_col_cnt = len(pi_snow_data)
            ora_col_cnt = len(ora_data)

            temp_is_match = set(temp_snow_data) == set(ora_data)
            pi_is_match = og_snow_data == [item for item in ora_data if item not in pi_snow_data]

            temp_snow_only_list = list(set(temp_snow_data) - set(ora_data))
            ora_only_list = list(set(ora_data) - set(temp_snow_data))

            snow_only = ','.join(temp_snow_only_list)
            ora_only = ','.join(ora_only_list)

            # 비교 결과를 Snowflake 테이블에 삽입
            insert_query = f"""
                INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_COLUMN_INFO_VERIFY (
                    VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                    TEMP_IS_MATCH, ORA_COL_CNT, TEMP_SNOW_COL_CNT, ORA_ONLY, TEMP_SNOW_ONLY,
                    PI_IS_MATCH, OG_SNOW_COL_CNT, PI_COL_CNT
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            parameters = (
                execution_date,
                snow_db_name,
                schema_name,
                table_name,
                temp_is_match,
                ora_col_cnt,
                snow_col_cnt,
                ora_only,
                snow_only,
                pi_is_match,
                og_col_cnt,
                pi_col_cnt
            )
            print(parameters)
            snowflake_cursor.execute(insert_query, parameters)

        # 커넥션 닫기
        snowflake_connection.commit()
        snowflake_cursor.close()
        snowflake_connection.close()


    # 태스크 간의 의존성 설정
    ora_push(table_list) >> snow_push(table_list) >> insert_result(table_list)
