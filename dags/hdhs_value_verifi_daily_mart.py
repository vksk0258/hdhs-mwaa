from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터
import pandas as pd  # 데이터프레임 작업을 위한 pandas

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")
KST = pendulum.timezone("Asia/Seoul")
execution_time = pendulum.now(KST)

def get_verification_dict():
    """
    DB에서 MART_TABLE_VERIFI_LIST 및 MART_TABLE_SUM_CHECK_LIST를 조회하여 딕셔너리를 생성하는 함수
    :param snowflake_conn_params: 스노우플레이크 연결 정보 (dict)
    :return: {SCHEMA_NAME.TABLE_NAME: [COLUMN_NAME, ...]} 형식의 딕셔너리
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    conn = snowflake_hook.get_conn()
    cur = conn.cursor()

    # MART_TABLE_VERIFI_LIST에서 VERIFICATION_YN = 'Y' 인 테이블 조회
    cur.execute("""
        SELECT SCHEMA_NAME, TABLE_NAME 
        FROM DW_LOAD_DB.VERIFI_DATA_MART.MART_TABLE_COUNT_VERIFI_LIST
        WHERE VERIFICATION_YN = 'Y'
    """)
    table_list = {f"{row[0]}.{row[1]}": [] for row in cur.fetchall()}

    if table_list:
        # MART_TABLE_SUM_CHECK_LIST에서 SUM_CHECK_YN = 'Y' 인 데이터 조회
        cur.execute("""
            SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME 
            FROM DW_LOAD_DB.VERIFI_DATA_MART.MART_TABLE_SUM_VERIFI_LIST
            WHERE SUM_CHECK_YN = 'Y'
        """)

        for schema_name, table_name, column_name in cur.fetchall():
            full_table_name = f"{schema_name}.{table_name}"
            if full_table_name in table_list:
                table_list[full_table_name].append(column_name)

    cur.close()
    conn.close()
    return table_list


# DAG 정의
with DAG(
    dag_id="hdhs_value_verifi_daily_mart",  # DAG의 고유 식별자
    start_date=pendulum.datetime(2025, 2, 3, tz="Asia/Seoul"),
    schedule_interval="0 8 * * *",
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=500),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","검증"]  # DAG에 붙일 태그
) as dag:

    # Oracle 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='oracle_value_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def ora_push(**kwargs):
        """
        Oracle 데이터베이스에서 각 테이블의 레코드 수와 (특정 테이블의 경우) 합계를 조회한 뒤,
        결과를 XCom으로 전달하는 태스크.
        """
        start_time_str = execution_time.replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = execution_time.replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')

        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_OCI', thick_mode=True, thick_mode_lib_dir=client_path)
        table_list = get_verification_dict()

        ti.xcom_push(key='verifi_list', value=table_list)

        oracle_connection = oracle_hook.get_conn()
        oracle_cursor = oracle_connection.cursor()

        value_list = []

        for table, columns in table_list.items():
            try:
                if columns:
                    sum_columns = ", ".join([f"SUM({col})" for col in columns])
                    ora_query = f"""SELECT COUNT(*), {sum_columns} 
                                    FROM {table}
                                    WHERE ETL_DTM >= TO_DATE('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                                    AND ETL_DTM < TO_DATE('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')"""
                    result = oracle_cursor.execute(ora_query).fetchone()
                    count_value = result[0]
                    sum_values = [[columns[i], result[i + 1]] for i in range(len(columns))]
                    value_list.append([count_value] + sum_values)
                else:
                    ora_query = f"""SELECT COUNT(*) 
                                    FROM {table}
                                    WHERE ETL_DTM >= TO_DATE('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                                    AND ETL_DTM < TO_DATE('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')"""
                    result = oracle_cursor.execute(ora_query).fetchone()
                    value_list.append([result[0]])

                print(ora_query)

                print(f"ORA RESULT : {value_list}")

                # 조회 결과를 XCom으로 전달
                ti.xcom_push(key='ora_data', value=value_list)
            except Exception as e:
                print(f"Error with value: {e}")

        # 커넥션 닫기
        oracle_cursor.close()
        oracle_connection.close()


    # Snowflake 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='snowflake_value_push_xcom')
    def snow_push(**kwargs):
        """
        Snowflake 데이터베이스에서 각 테이블의 레코드 수와 (특정 테이블의 경우) 합계를 조회한 뒤,
        결과를 XCom으로 전달하는 태스크.
        """
        start_time_str = execution_time.replace(hour=0, minute=0, second=0).strftime('%Y-%m-%d %H:%M:%S')
        end_time_str = execution_time.replace(hour=23, minute=59, second=59).strftime('%Y-%m-%d %H:%M:%S')

        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)

        table_list = ti.xcom_pull(key='verifi_list', task_ids='oracle_value_push_xcom')

        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        snowflake_connection = snowflake_hook.get_conn()
        snowflake_cursor = snowflake_connection.cursor()

        value_list = []  # 결과 저장 리스트

        # 테이블 목록 순회하며 쿼리 실행
        for table, columns in table_list.items():
            try:
                schema, table_name = table.split('.')
                if columns:
                    sum_columns = ", ".join([f"SUM({col})" for col in columns])
                    snow_query = f"""SELECT COUNT(*), {sum_columns} 
                                    FROM DW_OCI.{table_name}
                                    WHERE ETL_DTM >= TO_DATE('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                                    AND ETL_DTM < TO_DATE('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')"""
                    result = snowflake_cursor.execute(snow_query).fetchone()
                    count_value = result[0]
                    sum_values = [[columns[i], result[i + 1]] for i in range(len(columns))]
                    value_list.append([count_value] + sum_values)
                else:
                    snow_query = f"""SELECT COUNT(*) 
                                    FROM DW_OCI.{table_name}
                                    WHERE ETL_DTM >= TO_DATE('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                                    AND ETL_DTM < TO_DATE('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')"""
                    result = snowflake_cursor.execute(snow_query).fetchone()
                    value_list.append([result[0]])

                print(snow_query)


                print(f"SNOW RESULT : {value_list}")

                # 조회 결과를 XCom으로 전달
                ti.xcom_push(key='snow_data', value=value_list)
            except Exception as e:
                print(f"Error with value: {e}")

        # 커넥션 닫기
        snowflake_cursor.close()
        snowflake_connection.close()

    # Oracle과 Snowflake 데이터를 비교하고 결과를 Snowflake에 저장
    @task(task_id='insert_comparison_results')
    def insert_result(**kwargs):
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

        table_list = ti.xcom_pull(key='verifi_list', task_ids='oracle_value_push_xcom')

        print(table_list)
        print(type(table_list))


        # 테이블 목록 순회하며 비교 수행
        for idx,table in enumerate(table_list):
            try:
                schema_name = table.split('.')[0]  # 테이블의 스키마 이름
                table_name = table.split('.')[1]  # 테이블 이름

                # XCom에서 Oracle 및 Snowflake 조회 결과 가져오기
                snow_data = ti.xcom_pull(key='snow_data', task_ids='snowflake_value_push_xcom')[idx]
                ora_data = ti.xcom_pull(key='ora_data', task_ids='oracle_value_push_xcom')[idx]

                print(ora_data)
                print(snow_data)

                snow_cnt = int(snow_data[0])  # Snowflake 레코드 수
                ora_cnt = int(ora_data[0])  # Oracle 레코드 수

                snow_sum_list = snow_data[1:]
                ora_sum_list = ora_data[1:]

                # 특정 테이블의 경우 SUM 값도 비교
                if snow_sum_list and ora_sum_list:
                    for snow_sum, ora_sum in zip(snow_sum_list, ora_sum_list):
                        snow_col_name = snow_sum[0]
                        snow_sum_value = snow_sum[1] if snow_sum[1] is not None else 0
                        ora_col_name = ora_sum[0]
                        ora_sum_value = ora_sum[1] if ora_sum[1] is not None else 0
                        sum_insert_query = """
                                        INSERT INTO DW_LOAD_DB.VERIFI_DATA_MART.MART_DATA_SUM_VERIFI_LOG (
                                            VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME,
                                            ORA_SUM, SNOW_SUM
                                        )
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    """
                        parameters = (
                            execution_time,
                            snow_db_name,
                            schema_name,
                            table_name,
                            snow_col_name,
                            ora_sum_value,
                            snow_sum_value
                        )
                        snowflake_cursor.execute(sum_insert_query, parameters)



                # 비교 결과를 Snowflake 테이블에 삽입
                cnt_insert_query = """
                    INSERT INTO DW_LOAD_DB.VERIFI_DATA_MART.MART_DATA_COUNT_VERIFI_LOG (
                        VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, 
                        ORA_CNT, SNOW_CNT
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """
                parameters = (
                    execution_time,
                    snow_db_name,
                    schema_name,
                    table_name,
                    ora_cnt,
                    snow_cnt
                )
                snowflake_cursor.execute(cnt_insert_query, parameters)
            except Exception as e:
                print(f"Error with value: {e}")

        # 커넥션 닫기
        snowflake_connection.commit()
        snowflake_cursor.close()
        snowflake_connection.close()

    # 태스크 간의 의존성 설정
    ora_push() >> snow_push() >> insert_result()
