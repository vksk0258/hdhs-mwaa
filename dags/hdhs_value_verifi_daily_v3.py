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

# 테이블 목록 정의 (Oracle에서 비교할 대상 테이블들)
table_list = {
    "HDHS_OD.OD_ORD_DTL": ["ORD_NO", "ORD_PTC_SEQ", "CHG_DTM"],
    "HDHS_OD.OD_BASKT_INF": ["CUST_NO", "BASKT_SEQ", "CHG_DTM"],
    "HDHS_PD.IM_SLITM_PRC_DTL": ["ITEM_NO", "PRC_SEQ", "CHG_DTM"]
}

# Snowflake 테이블 접미사 정의
snowflake_suffixes = ["_352_BIN", "_353_BIN", "_354_BIN", "_352_LOG", "_353_LOG", "_354_LOG"]

# DAG 정의
with DAG(
    dag_id="hdhs_value_verifi_daily_v3",  # DAG의 고유 식별자
    schedule_interval=None,
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=60),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑", "검증"]  # DAG에 붙일 태그
) as dag:

    # Oracle 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='oracle_value_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def ora_push(table_list, snowflake_suffixes, **kwargs):
        """
        Oracle 데이터베이스에서 각 테이블의 레코드 수와 데이터를 Snowflake로 전달하며 비교.
        """
        ti = kwargs['ti']  # Task Instance (XCom 푸시를 위해 필요)

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
        snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snow_load")
        oracle_connection = oracle_hook.get_conn()
        snowflake_connection = snowflake_hook.get_conn()

        # Snowflake 엔진 생성
        snowflake_engine = SnowflakeHook(snowflake_conn_id="conn_snow_load").get_sqlalchemy_engine()

        # 데이터 시간 범위 가져오기
        previous_time = kwargs['data_interval_start'].in_tz(pendulum.timezone("Asia/Seoul"))
        current_time = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))
        current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
        previous_time_str = previous_time.strftime('%Y-%m-%d %H:%M:%S')

        # 테이블 목록 순회하며 쿼리 실행
        for table, columns in table_list.items():
            schema, table_name = table.split('.')
            key_columns = [col for col in columns if col != "CHG_DTM"]  # "CHG_DTM"을 제외한 키 컬럼
            key_condition = " AND ".join([f"sub.{col} = {table_name}.{col}" for col in key_columns])  # 키 조건 생성

            ora_query = f"""
                SELECT {', '.join(columns)}
                FROM {table}
                WHERE CHG_DTM = (
                    SELECT MAX(sub.CHG_DTM)
                    FROM {table} sub
                    WHERE {key_condition}
                    AND sub.CHG_DTM >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                    AND sub.CHG_DTM < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                )
                AND CHG_DTM >= TO_DATE('{previous_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                AND CHG_DTM < TO_DATE('{current_time_str}', 'YYYY-MM-DD HH24:MI:SS')
                """
            print(f"Query for table {table}:{ora_query}\n")

            # Oracle에서 데이터 조회
            ora_df = pd.read_sql(ora_query, con=oracle_connection)

            # Snowflake 테이블 접미사 순회하며 데이터 비교
            for suffix in snowflake_suffixes:
                snowflake_table = f"{table_name}{suffix}"
                snow_query = ora_query.replace(table, f"{schema}.{snowflake_table}")
                print(f"Query for Snowflake table {snowflake_table}:{snow_query}\n")

                snow_df = pd.read_sql(snow_query, con=snowflake_connection)

                # 데이터 비교
                diff_df = ora_df.merge(snow_df, how='outer', on=columns, indicator=True)

                only_in_ora_df = diff_df[diff_df['_merge'] == 'left_only']
                only_in_snow_df = diff_df[diff_df['_merge'] == 'right_only']

                # Snowflake에 데이터 적재
                for _, row in only_in_ora_df.iterrows():
                    snowflake_engine.execute(f"""
                        INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_VERIFY_DTL
                        (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, KEYS, CHG_DTM, ORA_YN)
                        VALUES ('기간계', '{schema}', '{table}', '{','.join([str(row[col]) for col in key_columns])}', '{row['CHG_DTM']}', 'Y')
                    """)

                for _, row in only_in_snow_df.iterrows():
                    snowflake_engine.execute(f"""
                        INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_VERIFY_DTL
                        (DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, KEYS, CHG_DTM, ORA_YN)
                        VALUES ('DW_LOAD_DB', '{schema}', '{snowflake_table}', '{','.join([str(row[col]) for col in key_columns])}', '{row['CHG_DTM']}', 'N')
                    """)

        oracle_connection.close()
        snowflake_connection.close()

    ora_push(table_list=table_list, snowflake_suffixes=snowflake_suffixes)
