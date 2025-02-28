from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

# 테이블 목록 정의 (Oracle과 Snowflake에서 비교할 대상 테이블들)
table_list = [
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],
    ['', ''],

]

# DAG 정의
with DAG(
    dag_id="hdhs_reverse_batch_verifi_daily",  # DAG의 고유 식별자
    schedule=None,  # DAG의 예약 일정 없음 (수동 실행)
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=60),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","검증"]  # DAG에 붙일 태그
) as dag:

    # Oracle 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='oracle_value_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def ora_push(table_list, **kwargs):
        """
        Oracle 데이터베이스에서 각 테이블의 레코드 수와 (특정 테이블의 경우) 합계를 조회한 뒤,
        결과를 XCom으로 전달하는 태스크.
        """
        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)
        value_list = []  # 결과 저장 리스트

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        oracle_cursor = oracle_connection.cursor()

        # 테이블 목록 순회하며 쿼리 실행
        for table in table_list:
            if table == "HDHS_OD.OD_ORD_DTL":
                # 특정 테이블의 경우 COUNT와 SUM 조회
                ora_query = "select count(*), SUM(LAST_STLM_AMT) from " + table
            else:
                # 일반 테이블의 경우 COUNT만 조회
                ora_query = "select count(*) from " + table
            value_list.append(oracle_cursor.execute(ora_query).fetchone())

        # 커넥션 닫기
        oracle_cursor.close()
        oracle_connection.close()

        # 조회 결과를 XCom으로 전달
        ti.xcom_push(key='ora_data', value=value_list)

    # Snowflake 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='snowflake_value_push_xcom')
    def snow_push(table_list, **kwargs):
        """
        Snowflake 데이터베이스에서 각 테이블의 레코드 수와 (특정 테이블의 경우) 합계를 조회한 뒤,
        결과를 XCom으로 전달하는 태스크.
        """
        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)
        value_list = []  # 결과 저장 리스트

        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        # 테이블 목록 순회하며 쿼리 실행
        for table in table_list:
            if table == "HDHS_OD.OD_ORD_DTL":
                # 특정 테이블의 경우 COUNT와 SUM 조회
                snow_query = "select count(*), SUM(LAST_STLM_AMT) from DW_LOAD_DB." + table
            else:
                # 일반 테이블의 경우 COUNT만 조회
                snow_query = "select count(*) from DW_LOAD_DB." + table
            value_list.append(snowflake_cursor.execute(snow_query).fetchone())

        # 커넥션 닫기
        snowflake_cursor.close()
        snowflake_connection.close()

        # 조회 결과를 XCom으로 전달
        ti.xcom_push(key='snow_data', value=value_list)

    # Oracle과 Snowflake 데이터를 비교하고 결과를 Snowflake에 저장
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
            snow_data = ti.xcom_pull(key='snow_data', task_ids='snowflake_value_push_xcom')[idx]
            ora_data = ti.xcom_pull(key='ora_data', task_ids='oracle_value_push_xcom')[idx]

            print(ora_data)

            snow_cnt = int(snow_data[0])  # Snowflake 레코드 수
            ora_cnt = int(ora_data[0])  # Oracle 레코드 수

            # 특정 테이블의 경우 SUM 값도 비교
            if table == "HDHS_OD.OD_ORD_DTL":
                snow_sum = int(snow_data[1])  # Snowflake SUM 값
                ora_sum = int(ora_data[1])  # Oracle SUM 값
                minus_sum = ora_sum - snow_sum  # SUM 차이
            else:
                snow_sum = 0
                ora_sum = 0
                minus_sum = 0

            # 레코드 수 차이 및 비율 계산
            minus_cnt = ora_cnt - snow_cnt
            DIFF_RANGE = (minus_cnt / ora_cnt) * 100 if ora_cnt > 0 else 0

            # 비교 결과를 Snowflake 테이블에 삽입
            insert_query = """
                INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_VERIFY (
                    VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, 
                    ORA_CNT, SNOW_CNT,
                    ORA_SUM, SNOW_SUM,
                    MINUS_CNT, MINUS_SUM, DIFF_RANGE
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            parameters = (
                execution_date,
                snow_db_name,
                schema_name,
                table_name,
                ora_cnt,
                snow_cnt,
                ora_sum,
                snow_sum,
                minus_cnt,
                minus_sum,
                DIFF_RANGE
            )
            snowflake_cursor.execute(insert_query, parameters)

        # 커넥션 닫기
        snowflake_connection.commit()
        snowflake_cursor.close()
        snowflake_connection.close()

    # 태스크 간의 의존성 설정
    ora_push(table_list) >> snow_push(table_list) >> insert_result(table_list)
