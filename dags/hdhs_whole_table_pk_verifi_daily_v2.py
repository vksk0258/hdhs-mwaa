from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook  # Snowflake 데이터베이스 연결을 위한 Airflow Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import pendulum  # 타임존 처리를 위한 라이브러리
import datetime  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터
import pandas as pd  # 데이터프레임 작업을 위한 pandas
import boto3
from sqlalchemy import create_engine
import os

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")
schema_list = ["HDHS_PD", "HDHS_CM", "HDHS_BM", "HDHS_DS", "HDHS_DW", "HDHS_OD", "HDHS_OM",
               "HDHS_MK", "HDHS_AC", "HDHS_AM", "HDHS_NSTD", "HDHS_CU", "HDHS_CS"]

#고유 batch_id 생성
kst = pendulum.timezone("Asia/Seoul")
batch_id = f"whole_table_daily_{pendulum.now(kst).format('YYYYMMDDHHmmss')}"
Variable.set("whole_table_daily_verifi_batch_id", batch_id)

# S3 설정
s3_client = boto3.client("s3")
S3_BUCKET = "hdhs-dw-migdata-s3"
S3_PREFIX = "dw/data_verifi/"  # 저장할 S3 경로 지정
SNOWFLAKE_STAGE = "@DW_LOAD_DB.CONFIG.STG_AWS_MWAA_DATA_VERIFI"

def get_filtered_table_list(snowflake_hook, schema_name):
    query = f"""
        SELECT CONCAT(A.OWNER, '.', A.TABLE_NAME) AS FULL_TABLE_NAME, A.COLUMN_NAME
          FROM DW_LOAD_DB.VERIFI_DATA.TB_KEY_V2 A
         WHERE 1=1
           AND A.OWNER LIKE '%HDHS%'
           AND TABLE_NAME NOT LIKE '%BIN'
           AND TABLE_NAME NOT LIKE '%LOG'
           AND TABLE_NAME NOT LIKE '%TEST'
           AND TABLE_NAME NOT LIKE '%OCI'
           AND A.OWNER = '{schema_name}'
           AND EXISTS (SELECT 1
                         FROM DW_LOAD_DB.INFORMATION_SCHEMA.COLUMNS B
                        WHERE A.OWNER = B.TABLE_SCHEMA
                          AND A.TABLE_NAME = B.TABLE_NAME
                          AND B.COLUMN_NAME = 'CHG_DTM')
    """
    with snowflake_hook.get_conn() as conn:
        df = pd.read_sql(query, conn)

    table_list = {}
    for full_table_name, group in df.groupby("FULL_TABLE_NAME"):
        table_list[full_table_name] = group["COLUMN_NAME"].tolist()

    return table_list

def split_dataframe(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]

def ora_push(schema_name):

    ################
    # 기본 환경 세팅 #
    ################

    # Snowflake Hook을 사용하여 연결 생성
    snowflake_hook = SnowflakeHook(snowflake_conn_id="conn_snowflake_load_verifi")
    table_list = get_filtered_table_list(snowflake_hook, schema_name)
    print(table_list)

    # Oracle Hook을 사용하여 연결 생성
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_OCI', thick_mode=True, thick_mode_lib_dir=client_path)
    oracle_connection = oracle_hook.get_conn()
    engine = snowflake_hook.get_sqlalchemy_engine()

    # 데이터 검증 시작/종료 시간 설정
    start_days_ago = pendulum.now(kst).subtract(days=1).start_of('day')
    start_hours_ago = pendulum.now(kst).subtract(hours=1)
    start_specific_time = pendulum.from_format("20250218000000", 'YYYYMMDDHHmmss', tz=kst)
    start_time = start_days_ago.format('YYYYMMDDHHmmss')

    end_days_ago = pendulum.now(kst).subtract(days=0).start_of('day')
    end_hours_ago = pendulum.now(kst).subtract(hours=1)
    end_specific_time = pendulum.from_format("20250220235959", 'YYYYMMDDHHmmss', tz=kst)
    end_time = end_days_ago.format('YYYYMMDDHHmmss')

    # 테이블 목록 순회하며 쿼리 실행
    for table in table_list.keys():
        try:
            schema, table_name = table.split('.')
            columns = table_list[table]  # 해당 테이블의 컬럼 리스트 가져오기

            ####################
            # 오라클 데이터 Load #
            ####################

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

                    create_table_query_endtime_after = f"""
                    CREATE OR REPLACE TABLE DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA_ENDTIME_AFTER (
                        {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                        CHG_DTM VARCHAR(14)
                    )
                    """
                    cursor.execute(create_table_query_endtime_after)
                    print(f"Table {table}_ENDTIME_AFTER created successfully")

            ##################################################
            # [오라클 1번] 시작, 종료 비교 대상 오라클 데이터 조회 #
            ##################################################
            chg_ext_ora_query = f"""
               SELECT {', '.join(columns)},TO_CHAR(CHG_DTM, 'YYYYMMDDHH24MISS')
                 FROM {table}
                WHERE CHG_DTM BETWEEN TO_DATE('{start_time}', 'YYYYMMDDHH24MISS') AND TO_DATE('{end_time}', 'YYYYMMDDHH24MISS')
            """

            print(chg_ext_ora_query)

            chg_ext_ora_df = pd.read_sql(chg_ext_ora_query, con=oracle_connection)
            print(chg_ext_ora_df)

            chg_ext_ora_df.rename(columns={"TO_CHAR(CHG_DTM,'YYYYMMDDHH24MISS')": "CHG_DTM"}, inplace=True)
            if not chg_ext_ora_df.empty:
                temp_max_chg_dtm = pd.to_datetime(chg_ext_ora_df['CHG_DTM']).max().strftime('%Y-%m-%d %H:%M:%S.%f')[
                                   :-3]
            else:
                temp_max_chg_dtm = None
            print(temp_max_chg_dtm)
            print(chg_ext_ora_df)

            # CSV로 저장
            csv_filename = f"""{table}_{pendulum.now(kst).format('YYYYMMDDHHmmss')}.csv"""
            print("\n[INFO] csv_filename :\n", csv_filename)

            csv_full_path = f"""{S3_PREFIX}{csv_filename}"""
            print("\n[INFO] csv_full_path :\n", csv_full_path)

            local_csv_path = f"/tmp/{csv_filename.split('/')[-1]}"
            print("\n[INFO] local_csv_path :\n", local_csv_path)

            chg_ext_ora_df.to_csv(local_csv_path, index=False)

            # S3에 업로드
            s3_client.upload_file(local_csv_path, S3_BUCKET, csv_full_path)

            # Snowflake에 COPY INTO 실행
            snowflake_stg_refresh_query = f"""
                ALTER STAGE DW_LOAD_DB.CONFIG.STG_AWS_MWAA_DATA_VERIFI REFRESH
            """
            print(snowflake_stg_refresh_query)

            snowflake_copy_into_query = f"""
                COPY INTO VERIFI_DATA.TEMP_{table_name}_ORA
                FROM {SNOWFLAKE_STAGE}
                FILES = ('/{csv_filename}')
                FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER = 1)
            """
            print(snowflake_copy_into_query)
            with snowflake_hook.get_conn() as snowflake_conn:
                with snowflake_conn.cursor() as cursor:
                    cursor.execute(snowflake_stg_refresh_query)
                    cursor.execute(snowflake_copy_into_query)

            # S3 / local 에서 파일 삭제
            s3_client.delete_object(Bucket=S3_BUCKET, Key=csv_full_path)
            os.remove(local_csv_path)

            #############################################################
            # [오라클 2번] 종료 시간 이후 데이터 확인(SNOW_ONLY에서 제외 용도) #
            #############################################################
            chg_ext_ora_endtime_after_query = f"""
               SELECT {', '.join(columns)},TO_CHAR(CHG_DTM, 'YYYYMMDDHH24MISS')
                 FROM {table}
                WHERE CHG_DTM >= TO_DATE('{end_time}', 'YYYYMMDDHH24MISS')
                ORDER BY {', '.join(columns)}
            """

            chg_ext_ora_endtime_after_df = pd.read_sql(chg_ext_ora_endtime_after_query, con=oracle_connection)
            print(chg_ext_ora_endtime_after_df)

            chg_ext_ora_endtime_after_df.rename(columns={"TO_CHAR(CHG_DTM,'YYYYMMDDHH24MISS')": "CHG_DTM"},
                                                inplace=True)
            if not chg_ext_ora_endtime_after_df.empty:
                temp_max_chg_dtm_endtime_after = pd.to_datetime(
                    chg_ext_ora_endtime_after_df['CHG_DTM']).max().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            else:
                temp_max_chg_dtm_endtime_after = None
            print(temp_max_chg_dtm_endtime_after)
            print(chg_ext_ora_endtime_after_df)

            # CSV로 저장
            csv_filename = f"""{table}_{pendulum.now(kst).format('YYYYMMDDHHmmss')}_after.csv"""
            print("\n[INFO] csv_filename :\n", csv_filename)

            csv_full_path = f"""{S3_PREFIX}{csv_filename}"""
            print("\n[INFO] csv_full_path :\n", csv_full_path)

            local_csv_path = f"/tmp/{csv_filename.split('/')[-1]}"
            print("\n[INFO] local_csv_path :\n", local_csv_path)

            chg_ext_ora_df.to_csv(local_csv_path, index=False)

            # S3에 업로드
            s3_client.upload_file(local_csv_path, S3_BUCKET, csv_full_path)

            # Snowflake에 COPY INTO 실행
            snowflake_stg_refresh_query = f"""
                ALTER STAGE DW_LOAD_DB.CONFIG.STG_AWS_MWAA_DATA_VERIFI REFRESH
            """
            print(snowflake_stg_refresh_query)

            snowflake_copy_into_query = f"""
                COPY INTO VERIFI_DATA.TEMP_{table_name}_ORA_ENDTIME_AFTER
                FROM {SNOWFLAKE_STAGE}
                FILES = ('/{csv_filename}')
                FILE_FORMAT = (TYPE = CSV, FIELD_OPTIONALLY_ENCLOSED_BY='"', SKIP_HEADER = 1)
            """
            print(snowflake_copy_into_query)
            with snowflake_hook.get_conn() as snowflake_conn:
                with snowflake_conn.cursor() as cursor:
                    cursor.execute(snowflake_stg_refresh_query)
                    cursor.execute(snowflake_copy_into_query)

            # S3 / local 에서 파일 삭제
            s3_client.delete_object(Bucket=S3_BUCKET, Key=csv_full_path)
            os.remove(local_csv_path)

            #######################
            # [오라클 PK 저장 완료] #
            #######################

            with snowflake_hook.get_conn() as snowflake_conn:
                with snowflake_conn.cursor() as cursor:

                    # SNOW에 존재하는 데이터 COUNT 확인
                    snow_total_query = f"""
                        SELECT COUNT(*)
                          FROM DW_LOAD_DB.{schema}.{table_name}
                         WHERE CHG_DTM BETWEEN TO_TIMESTAMP('{start_time}', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{end_time}', 'YYYYMMDDHH24MISS')
                    """
                    print(snow_total_query)
                    cursor.execute(snow_total_query)
                    snow_total_result = cursor.fetchone()
                    snow_total_count = snow_total_result[0]
                    print(snow_total_count)

                    ###################
                    # 데이터 DIFF 검증 #
                    ###################

                    # SNOW와 ORACLE 데이터의 CHG_DTM 비교
                    diff_check_query = f"""
                        SELECT {', '.join(columns)}, ORACLE_CHG_DTM, CHG_DTM AS SNOW_CHG_DTM
                        FROM (SELECT {', '.join([f'A.{col}' for col in columns])},
                                   A.CHG_DTM,
                                   to_timestamp(B.CHG_DTM, 'YYYYMMDDHH24MISS') AS ORACLE_CHG_DTM,
                                   DATEDIFF('SECOND', A.CHG_DTM, ORACLE_CHG_DTM) AS SECOND_DIFF
                              FROM DW_LOAD_DB.{schema}.{table_name} A FULL OUTER JOIN DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA B 
                              ON {' AND '.join([f'A.{col} = B.{col}' for col in columns])}
                             WHERE A.CHG_DTM BETWEEN TO_TIMESTAMP('{start_time}', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{end_time}', 'YYYYMMDDHH24MISS') ) A
                        WHERE 1=1 
                        AND (SECOND_DIFF IS NULL OR SECOND_DIFF != 0)
                        AND ORACLE_CHG_DTM <= (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.{schema}.{table_name})
                        AND NOT EXISTS (SELECT 1 
                                        FROM DW_LOAD_DB.TEMP.TEMP_{table_name} B
                                        WHERE 1=1
                                        AND {' AND '.join([f'A.{col} = B.{col}' for col in columns])}
                                        AND B.OP = 'D' )
                        ORDER BY ORACLE_CHG_DTM ASC
                    """
                    print(diff_check_query)
                    cursor.execute(diff_check_query)
                    diff_results = cursor.fetchall()
                    print(diff_results)

                    # DIFF 로그 테이블 생성 및 데이터 삽입
                    log_table_name = f"TEMP_{table_name}_DIFF_LOG"
                    create_log_table_query = f"""
                    CREATE TABLE IF NOT EXISTS DW_LOAD_DB.VERIFI_DATA.{log_table_name} (
                        BATCH_ID VARCHAR(50), {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                        ORACLE_CHG_DTM TIMESTAMP,
                        SNOW_CHG_DTM TIMESTAMP,
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
                        print(insert_log_query)
                        cursor.execute(insert_log_query)

                    ########################
                    # ORACLE ONLY 확인 검증 #
                    ########################

                    only_ora_query = f"""
                        SELECT {', '.join(columns)}, to_timestamp(CHG_DTM, 'YYYYMMDDHH24MISS') AS ORA_CHG_DTM
                        FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA A
                        WHERE ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} 
                                                               FROM DW_LOAD_DB.{schema}.{table_name}
                                                              WHERE CHG_DTM BETWEEN TO_TIMESTAMP('{start_time}', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{end_time}', 'YYYYMMDDHH24MISS') )
                          AND to_timestamp(CHG_DTM, 'YYYYMMDDHH24MISS') < (SELECT MAX(CHG_DTM) FROM DW_LOAD_DB.{schema}.{table_name})
                        ORDER BY CHG_DTM ASC
                    """
                    print(only_ora_query)
                    cursor.execute(only_ora_query)
                    only_ora_results = cursor.fetchall()

                    # 오라클 Only 로그 테이블 생성
                    outer_log_table_name = f"TEMP_{table_name}_ORA_ONLY_LOG"
                    create_outer_log_table_query = f"""
                        CREATE TABLE IF NOT EXISTS DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name} (
                            BATCH_ID VARCHAR(50), {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                            ORA_CHG_DTM TIMESTAMP,
                            SNOW_CHG_DTM TIMESTAMP,
                            verifi_date VARCHAR(20)
                        )
                    """
                    cursor.execute(create_outer_log_table_query)

                    # Oracle 데이터 삽입
                    if only_ora_results:
                        values = []
                        for row in only_ora_results:
                            formatted_row = ", ".join([f"'{value}'" for value in row[:-1]])
                            values.append(f"('{batch_id}',{formatted_row}, '{row[-1]}', NULL, '{start_time}')")
                        insert_outer_log_query = f"""
                        INSERT INTO DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name}
                        VALUES {', '.join(values)}
                        """
                        print(insert_outer_log_query)
                        cursor.execute(insert_outer_log_query)

                    # 배치 아이디 기준으로 ORACLE에만 존재하는 데이터의 검증 쿼리 (MWAA에서 수행은 X, 다음번 MERGE 이후에 검증)
                    only_ora_query_by_batchid = f"""
                        SELECT A.*
                        FROM DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name} A
                        WHERE 1=1
                          AND BATCH_ID = '{batch_id}'
                          AND NOT EXISTS (SELECT 1
                                            FROM DW_LOAD_DB.{schema}.{table_name} B
                                           WHERE 1=1
                                             AND {' AND '.join([f'A.{col} = B.{col}' for col in columns])})
                    """
                    print(only_ora_query_by_batchid)

                    ######################
                    # SNOW ONLY 검증 로직 #
                    ######################

                    only_snow_query = f"""
                        SELECT {', '.join(columns)}, CHG_DTM AS SNOW_CHG_DTM
                        FROM DW_LOAD_DB.{schema}.{table_name} A
                        WHERE ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA)
                          AND ({', '.join(columns)}) NOT IN (SELECT {', '.join(columns)} FROM DW_LOAD_DB.VERIFI_DATA.TEMP_{table_name}_ORA_ENDTIME_AFTER)
                          AND CHG_DTM BETWEEN TO_TIMESTAMP('{start_time}', 'YYYYMMDDHH24MISS') AND TO_TIMESTAMP('{end_time}', 'YYYYMMDDHH24MISS')
                        ORDER BY CHG_DTM ASC
                    """
                    print(only_snow_query)
                    cursor.execute(only_snow_query)
                    only_snow_results = cursor.fetchall()

                    # Snow Only 로그 테이블 생성
                    outer_log_table_name = f"TEMP_{table_name}_SNOW_ONLY_LOG"
                    create_outer_log_table_query = f"""
                        CREATE TABLE IF NOT EXISTS DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name} (
                            BATCH_ID VARCHAR(50), {', '.join([f'{col} VARCHAR(50)' for col in columns])},
                            ORA_CHG_DTM TIMESTAMP,
                            SNOW_CHG_DTM TIMESTAMP,
                            verifi_date VARCHAR(20)
                        )
                    """
                    cursor.execute(create_outer_log_table_query)

                    # Snowflake 데이터 삽입
                    if only_snow_results:
                        values = []
                        for row in only_snow_results:
                            formatted_row = ", ".join([f"'{value}'" for value in row[:-1]])
                            values.append(f"('{batch_id}',{formatted_row}, NULL, '{row[-1]}', '{start_time}')")
                        insert_outer_log_query = f"""
                        INSERT INTO DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name}
                        VALUES {', '.join(values)}
                        """
                        print(insert_outer_log_query)
                        cursor.execute(insert_outer_log_query)

                    # 배치 아이디 기준으로 SNOWFLAKE에만 존재하는 데이터의 검증 쿼리 (MWAA에서 수행은 X, 다음번 MERGE 이후에 검증)
                    only_snow_query_by_batchid = f"""
                        SELECT A.*
                        FROM DW_LOAD_DB.VERIFI_DATA.{outer_log_table_name} A
                        WHERE 1=1
                          AND BATCH_ID = '{batch_id}'
                          AND EXISTS (SELECT 1
                                        FROM DW_LOAD_DB.{schema}.{table_name} B
                                       WHERE 1=1
                                         AND {' AND '.join([f'A.{col} = B.{col}' for col in columns])})
                    """
                    print(only_snow_query_by_batchid)

                    execution_dtm = kst_now.to_datetime_string()

                    dtm_obj = datetime.datetime.strptime(start_time, '%Y%m%d%H%M%S')
                    formatted_dtm = dtm_obj.strftime('%Y-%m-%d %H:%M:%S')

                    insert_verifi_log_query = f"""
                        INSERT INTO DW_LOAD_DB.VERIFI_DATA.CDC_DATA_VERIFI_LOG (
                            BATCH_ID,
                            TABLE_NM,
                            SCHEMA_NAME,
                            VERIFI_DATE,
                            ORACLE_TOTAL_COUNT,
                            SNOW_TOTAL_COUNT,
                            CHG_DTM_DIFF_COUNT,
                            CHG_DTM_DIFF_QUERY,
                            ORACLE_ONLY_COUNT,
                            ORACLE_ONLY_QUERY,
                            ORACLE_ONLY_QUERY_BY_BATCH_ID,
                            SNOW_ONLY_COUNT,
                            SNOW_ONLY_QUERY,
                            SNOW_ONLY_QUERY_BY_BATCH_ID,
                            EXECUTE_DTM,
                            TEMP_MAX_CHG_DTM
                        ) VALUES (
                            '{batch_id}',
                            '{table_name}', 
                            '{schema}', 
                            '{formatted_dtm}', 
                            {len(chg_ext_ora_df)},
                            {snow_total_count},
                            {len(diff_results)},
                            '{diff_check_query.replace("'", "''")}',
                            {len(only_ora_results)}, 
                            '{only_ora_query.replace("'", "''")}' , 
                            '{only_ora_query_by_batchid.replace("'", "''")}' , 
                            {len(only_snow_results)}, 
                            '{only_snow_query.replace("'", "''")}',
                            '{only_snow_query_by_batchid.replace("'", "''")}',
                            '{execution_dtm}', 
                            {f"'{temp_max_chg_dtm}'" if temp_max_chg_dtm else 'null'}
                        )
                    """
                    cursor.execute(insert_verifi_log_query)

        except Exception as e:
            print(f"Error with value: {e}")

    oracle_connection.close()

# DAG 정의
with DAG(
    dag_id="hdhs_whole_table_pk_verifi_daily_v2",  # DAG의 고유 식별자
    start_date=pendulum.datetime(2025, 2, 27, tz="Asia/Seoul"),
    schedule_interval="15 1 * * *",
    catchup=False,  # 과거 데이터 실행을 스킵
    dagrun_timeout=datetime.timedelta(minutes=180),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑", "POC", "검증"]  # DAG에 붙일 태그
) as dag:

    trigger_hdhs_whole_table_pk_error_merge = TriggerDagRunOperator(
        task_id='trigger_hdhs_whole_table_pk_error_merge',
        trigger_dag_id='hdhs_whole_table_pk_error_merge',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    tasks = []
    for schema_name in schema_list:
        task = PythonOperator(
            task_id=f"process_{schema_name.replace('.', '_')}",
            python_callable=ora_push,
            op_args=[schema_name],
            retries=10,
            retry_delay=datetime.timedelta(seconds=10),
            trigger_rule="all_done"
        )
        tasks.append(task)

    for i in range(len(tasks) - 1):
        tasks.append(tasks[i])

    tasks >> trigger_hdhs_whole_table_pk_error_merge