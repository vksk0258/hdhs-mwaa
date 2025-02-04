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

def get_filtered_table_list(snowflake_hook):
    query = """
        SELECT CONCAT(OWNER, '.', TABLE_NAME) AS FULL_TABLE_NAME, COLUMN_NAME
        FROM VERIFI_DATA.TB_KEY_V2
        WHERE VERIFICATION_YN = 'Y'
    """
    with snowflake_hook.get_conn() as conn:
        df = pd.read_sql(query, conn)

    table_list = {}
    for full_table_name, group in df.groupby("FULL_TABLE_NAME"):
        table_list[full_table_name] = group["COLUMN_NAME"].tolist()


    print(table_list)

    return table_list

def split_dataframe(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]

# DAG 정의
with DAG(
    dag_id="hdhs_value_verifi_daily_v4",  # DAG의 고유 식별자
    start_date=pendulum.datetime(2025, 1, 22, tz="Asia/Seoul"),
    schedule_interval="0 7 * * *",
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=1000),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑", "검증"]  # DAG에 붙일 태그
) as dag:

    # Oracle 데이터 조회 및 XCom으로 데이터 전달
    @task(task_id='oracle_value_push_xcom', retries=10, retry_delay=datetime.timedelta(seconds=10))
    def ora_push():
        # Snowflake Hook을 사용하여 연결 생성
        snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snowflake_load_verifi")
        table_list = get_filtered_table_list(snowflake_hook)
        print(table_list)

        # Oracle Hook을 사용하여 연결 생성
        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_OCI', thick_mode=True, thick_mode_lib_dir=client_path)
        oracle_connection = oracle_hook.get_conn()
        engine = snowflake_hook.get_sqlalchemy_engine()

        kst = pendulum.timezone("Asia/Seoul")
        yesterday_start = pendulum.now(kst).subtract(days=1).start_of('day')
        start_time = yesterday_start.format('YYYYMMDDHHmmss')

        # 테이블 목록 순회하며 쿼리 실행
        for table, columns in table_list.items():
            try:
                schema, table_name = table.split('.')

                # 테이블별 고유 batch_id 생성
                batch_id = f"{table_name}_{pendulum.now(kst).format('YYYYMMDDHHmmss')}"
                print(f"Generated batch_id for table {table}: {batch_id}")

                kst_now = pendulum.now("Asia/Seoul")

                # 테이블 생성 확인 및 처리
                with snowflake_hook.get_conn() as snowflake_conn:
                    with snowflake_conn.cursor() as cursor:
                        create_table_query = f"""
                        CREATE OR REPLACE TABLE DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA (
                            {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                            CHG_DTM VARCHAR(14)
                        )
                        """
                        cursor.execute(create_table_query)
                        print(f"Table {table} created successfully")

                chg_ext_ora_query = f"""
                    SELECT {', '.join(columns)},TO_CHAR(CHG_DTM, 'YYYYMMDDHH24MISS')
                    FROM {table}
                    WHERE CHG_DTM >= TO_DATE('{start_time}', 'YYYYMMDDHH24MISS')
                    ORDER BY {', '.join(columns)}
                """

                print(chg_ext_ora_query)

                # Oracle에서 데이터 조회
                chg_ext_ora_df = pd.read_sql(chg_ext_ora_query, con=oracle_connection)
                print(chg_ext_ora_df)

                chg_ext_ora_df.rename(columns={"TO_CHAR(CHG_DTM,'YYYYMMDDHH24MISS')": "CHG_DTM"}, inplace=True)
                if not chg_ext_ora_df.empty:
                    temp_max_chg_dtm = pd.to_datetime(chg_ext_ora_df['CHG_DTM']).max().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                else:
                    temp_max_chg_dtm = None
                print(temp_max_chg_dtm)

                print(chg_ext_ora_df)

                batch_size = 100000
                for batch_df in split_dataframe(chg_ext_ora_df, batch_size):
                    batch_df.to_sql(f"TEMP_{table_name}_ORA", con=engine, schema="VERIFI_DATA", if_exists='append',
                                    index=False)

                with snowflake_hook.get_conn() as snowflake_conn:
                    with snowflake_conn.cursor() as cursor:
                        diff_check_query = f"""
                            SELECT {', '.join(columns)}, ORACLE_CHG_DTM
                            FROM (SELECT {', '.join([f'A.{col}' for col in columns])},
                                       A.CHG_DTM,
                                       to_timestamp(B.CHG_DTM, 'YYYYMMDDHH24MISS') AS ORACLE_CHG_DTM,
                                       DATEDIFF('SECOND', A.CHG_DTM, ORACLE_CHG_DTM) AS SECOND_DIFF
                                  FROM DW_LOAD_DB.{schema}.{table_name} A FULL OUTER JOIN DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA B 
                                  ON {' AND '.join([f'A.{col} = B.{col}' for col in columns])}
                                 WHERE A.CHG_DTM  >= TO_DATE('{start_time}', 'YYYYMMDDHH24MISS') ) A
                            WHERE 1=1 
                            AND (SECOND_DIFF IS NULL OR SECOND_DIFF != 0)
                            AND ORACLE_CHG_DTM <= (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.{schema}.{table_name})
                            AND NOT EXISTS (SELECT 1 
                                            FROM DW_LOAD_DB.TEMP.TEMP_{table_name} B
                                            WHERE 1=1
                                            AND {' AND '.join([f'A.{col} = B.{col}' for col in columns])}
                                            AND B.OP = 'D' 
                                            AND B.CHG_DTM > (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.{schema}.{table_name}))
                            ORDER BY ORACLE_CHG_DTM ASC
                        """
                        print(diff_check_query)
                        cursor.execute(diff_check_query)
                        diff_results = cursor.fetchall()
                        print(diff_results)

                        # 로그 테이블 생성 및 데이터 삽입
                        log_table_name = f"TEMP_{table_name}_VERIFI_LOG"
                        create_log_table_query = f"""
                        CREATE TABLE IF NOT EXISTS DW_LOAD_DB.VERIFI_DATA.{log_table_name} (
                            BATCH_ID VARCHAR(50), {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                            ORACLE_CHG_DTM TIMESTAMP,
                            verifi_date VARCHAR(20)
                        )
                        """
                        print(create_log_table_query)
                        cursor.execute(create_log_table_query)

                        if diff_results:
                            values = []
                            for row in diff_results:
                                formatted_row = ", ".join([f"'{value}'" for value in row])
                                values.append(f"('{batch_id}',{formatted_row}, '{start_time}')")

                            insert_log_query = f"""
                            INSERT INTO DW_LOAD_DB.VERIFI_DATA.{log_table_name}
                            VALUES {', '.join(values)}
                            """
                            cursor.execute(insert_log_query)

                        only_ora_query = f"""
                            SELECT {', '.join(columns)}, to_timestamp(CHG_DTM, 'YYYYMMDDHH24MISS') AS ORA_CHG_DTM
                            FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA
                            WHERE ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} FROM DW_LOAD_DB.{schema}.{table_name})
                              AND to_timestamp(CHG_DTM, 'YYYYMMDDHH24MISS') < (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.TEMP.TEMP_{table_name})
                            ORDER BY CHG_DTM ASC
                        """
                        print(only_ora_query)
                        cursor.execute(only_ora_query)
                        only_ora_results = cursor.fetchall()

                        only_snow_query = f"""
                            SELECT {', '.join(columns)}, CHG_DTM AS SNOW_CHG_DTM
                            FROM DW_LOAD_DB.{schema}.{table_name} A
                            WHERE ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA)
                              AND CHG_DTM >= TO_DATE('{start_time}', 'YYYYMMDDHH24MISS')
                              AND NOT EXISTS (SELECT 1 
                                            FROM DW_LOAD_DB.TEMP.TEMP_{table_name} B
                                            WHERE 1=1
                                            AND {' AND '.join([f'A.{col} = B.{col}' for col in columns])}
                                            AND B.OP = 'D' 
                                            AND B.CHG_DTM >= (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.{schema}.{table_name}))
                            ORDER BY CHG_DTM ASC
                        """
                        print(only_snow_query)
                        cursor.execute(only_snow_query)
                        only_snow_results = cursor.fetchall()

                        # 로그 테이블 생성
                        outer_log_table_name = f"TEMP_{table_name}_outer_log"
                        create_outer_log_table_query = f"""
                            CREATE TABLE IF NOT EXISTS DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name} (
                                BATCH_ID VARCHAR(50), {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                                ORA_CHG_DTM TIMESTAMP,
                                SNOW_CHG_DTM TIMESTAMP,
                                verifi_date VARCHAR(20),
                                ORA_ONLY_YN VARCHAR(2)
                            )
                        """
                        cursor.execute(create_outer_log_table_query)

                        # Oracle 데이터 삽입
                        if only_ora_results:
                            values = []
                            for row in only_ora_results:
                                formatted_row = ", ".join([f"'{value}'" for value in row[:-1]])
                                values.append(f"('{batch_id}',{formatted_row}, '{row[-1]}', NULL, '{start_time}', 'Y')")
                            insert_outer_log_query = f"""
                            INSERT INTO DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name}
                            VALUES {', '.join(values)}
                            """
                            cursor.execute(insert_outer_log_query)

                        # Snowflake 데이터 삽입
                        if only_snow_results:
                            values = []
                            for row in only_snow_results:
                                formatted_row = ", ".join([f"'{value}'" for value in row[:-1]])
                                values.append(f"('{batch_id}',{formatted_row}, NULL, '{row[-1]}', '{start_time}', 'N')")
                            insert_outer_log_query = f"""
                            INSERT INTO DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name}
                            VALUES {', '.join(values)}
                            """
                            cursor.execute(insert_outer_log_query)

                        execution_dtm = kst_now.to_datetime_string()

                        dtm_obj = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S')
                        formatted_dtm = dtm_obj.strftime('%Y-%m-%d %H:%M:%S')

                        insert_verifi_log_query = f"""
                            INSERT INTO DW_LOAD_DB.VERIFI_DATA.CDC_DATA_VERIFI_LOG (
                                BATCH_ID, TABLE_NM, SCHEMA_NAME, VERIFI_DATE,
                                CHG_DTM_DIFF_COUNT,
                                CHG_DTM_DIFF_QUERY,
                                ORACLE_ONLY_COUNT,
                                ORACLE_ONLY_QUERY,
                                SNOW_ONLY_COUNT,
                                SNOW_ONLY_QUERY,
                                EXECUTE_DTM,
                                TEMP_MAX_CHG_DTM
                            ) VALUES (
                                '{batch_id}',
                                '{table_name}', 
                                '{schema}', 
                                '{formatted_dtm}', 
                                {len(diff_results)},
                                '{diff_check_query.replace("'", "''")}',
                                {len(only_ora_results)}, 
                                '{only_ora_query.replace("'", "''")}' , 
                                {len(only_snow_results)}, 
                                '{only_snow_query.replace("'", "''")}',
                                '{execution_dtm}', 
                                {f"'{temp_max_chg_dtm}'" if temp_max_chg_dtm else 'null'}
                            )
                        """
                        cursor.execute(insert_verifi_log_query)

            except Exception as e:
                print(f"Error with value: {e}")

        oracle_connection.close()

    ora_push()
