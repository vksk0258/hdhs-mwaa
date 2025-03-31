from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
import numpy as np
from common.notify_error_functions import notify_api_on_error
import pendulum
from datetime import datetime, timedelta
import pandas as pd
import boto3
import json
import os

# Column definitions
MD_VEN_INTL_SETUP_DTL_COLUMNS = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
ITEM_INTL_DTL_COLUMNS = ["ALML_CD", "SLITM_CD", "ALML_ITEM_CD", "ALML_CO_GBCD", "ALML_INTL_NO", "SOON_USE_PRMO_NO", "SOON_USE_PRMO_PRC", "ADD_DC_PRMO_NO", "ADD_DC_PRMO_PRC", "SELL_PRC", "ALML_SELL_GBCD", "SELL_GBCD", "ITNT_DISP_YN", "ITEM_PRC_APLY_DTM", "VEN_CD", "VEN2_CD", "OSHP_VEN_ADR_SEQ", "RTP_EXCH_VEN_ADR_SEQ", "SDLVC_VEN_SEQ", "DLVC_PAY_GBCD", "BNDL_DLVC_GBCD", "NCHG_DLV_BSIC_AMT", "DLVC_BSIC_QTY", "DLV_COST", "RTP_DLV_COST", "EXCH_DLV_COST", "SEND_DTM", "ALML_INTL_RST_GBCD", "ALML_ERR_CD", "ALML_ERR_MSG", "ORGL_ALML_ITEM_CD", "ALML_APRVL_STAT_CD", "ALML_PRC_APRVL_STAT_CD", "RJT_PTC_RSN", "RPROC_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM", "ALML_ADD_CNTN"]
INTL_EXCP_SETUP_DTL_COLUMNS = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "ITEM_INTL_GBCD", "ITEM_INTL_PTC_CD", "RMRK", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]

INTL_EXCP_SETUP_DTL_pk=["ALML_CD", "ITEM_INTL_GBCD", "ITEM_INTL_PTC_CD", "MD_CD", "VEN2_CD", "VEN_CD"]
ITEM_INTL_DTL_pk=["ALML_CD", "SLITM_CD"]

KST = pendulum.timezone("Asia/Seoul")

# Constants
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
TMP_DIR = "/tmp/postgre_incremental"
TABLE_NAME_LIST = [
    "ODS_ALLI.AM_ALML_MD_VEN_INTL_SETUP_DTL",
    "ODS_ALLI.AM_ALML_INTL_EXCP_SETUP_DTL",
    "ODS_ALLI.AM_ALML_ITEM_INTL_DTL"
]

# Load parameters from S3
s3 = boto3.client('s3')
PARAM_BUCKET_NAME = "hdhs-dw-mwaa-s3"
PARAM_KEY = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=PARAM_BUCKET_NAME, Key=PARAM_KEY)
params = json.load(response['Body'])

# Parse time parameters
p_start = f"{params.get('$$P_START')}000000"
p_end = f"{params.get('$$P_END')}235959"

fm_p_start = datetime.strptime(p_start, "%Y%m%d%H%M%S")
fm_p_end = datetime.strptime(p_end, "%Y%m%d%H%M%S")


postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hdhs_reading')
snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_load_ods')

def delete_existing_files(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        print(f"Deleted existing files in S3 path: s3://{bucket_name}/{prefix}")
    else:
        print(f"No existing files to delete in S3 path: s3://{bucket_name}/{prefix}")

BATCH_SIZE = 200000

def process_in_batches(table, columns, pk_columns):

    s3_bucket_name = "hdhs-dw-migdata-s3"

    schema, table_name = table.split('.')

    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/postgre_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)

    with postgres_hook.get_conn() as postgres_conn:
        print("==================[ORACLE QEURY]==================")
        query = f"""
                SELECT *
                FROM {table_name}
                WHERE CHG_DTM >= TO_TIMESTAMP('{params.get('$$P_START')}' || '000000', 'YYYYMMDDHH24MISS') 
                AND CHG_DTM <= TO_TIMESTAMP('{params.get('$$P_END')}' || '235959', 'YYYYMMDDHH24MISS')
            """
        print(query)

        df = pd.read_sql(query, postgres_conn)

    if df.empty:
        print("No more data to process for this table.")
        return None

    print(f"## {table} 조회 카운트 : {len(df)}")

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    # NaN 값을 명확하게 None으로 변환
    df.replace({np.nan: None}, inplace=True)

    values = df.where(pd.notnull(df), None).values.tolist()

    batch_size = 50000  # 한 번에 실행할 최대 행 수

    chunk_index = 1

    for i in range(0, len(values), batch_size):

        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:

            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            # PARTITION BY 구문을 자동으로 만듦
            partition_key = ", ".join(pk_columns)
            order_key = "CHG_DTM DESC"  # 정렬 기준은 필요에 따라 바꾸세요

            merge_query = f"""
                MERGE INTO {schema}.{table_name} AS target
                USING (
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY {partition_key} ORDER BY {order_key}) AS rn
                        FROM {schema}.{temp_table_name}
                    ) sub
                    WHERE rn = 1
                ) AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values});
            """

            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            print(f"{temp_table_name} 머지 완료!!!!")
def process_in_batches_sep_hour(table, columns, pk_columns):

    s3_bucket_name = "hdhs-dw-migdata-s3"

    schema, table_name = table.split('.')

    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/postgre_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)

    chunk_index = 1

    with postgres_hook.get_conn() as postgres_conn:
        print("==================[ORACLE QEURY]==================")

        # 날짜 문자열을 datetime 객체로 변환
        start_date_str = params.get('$$P_START')
        start_date = datetime.strptime(start_date_str, "%Y%m%d")

        for hour in range(24):
            start_time = start_date + timedelta(hours=hour)
            end_time = start_time + timedelta(hours=1) - timedelta(seconds=1)

            start_str = start_time.strftime('%Y%m%d%H%M%S')
            end_str = end_time.strftime('%Y%m%d%H%M%S')

            query = f"""
                SELECT *
                FROM {table_name}
                WHERE CHG_DTM >= TO_TIMESTAMP('{start_str}', 'YYYYMMDDHH24MISS')
                AND CHG_DTM <= TO_TIMESTAMP('{end_str}', 'YYYYMMDDHH24MISS')
            """
            print(query)

            df = pd.read_sql(query, postgres_conn)

            if df.empty:
                print("No more data to process for this table.")
                return None

            print(f"## {table} 조회 카운트 : {len(df)}")

            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                    else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                )

            # NaN 값을 명확하게 None으로 변환
            df.replace({np.nan: None}, inplace=True)

            s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
            file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

            df.to_parquet(file_name, engine='pyarrow', index=False)
            print(f"Data batch {chunk_index} saved to {file_name}")

            os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
            print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

            os.remove(file_name)
            print(f"Deleted {file_name} from local directory")

            chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:

            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            # PARTITION BY 구문을 자동으로 만듦
            partition_key = ", ".join(pk_columns)
            order_key = "CHG_DTM DESC"  # 정렬 기준은 필요에 따라 바꾸세요

            merge_query = f"""
                MERGE INTO {schema}.{table_name} AS target
                USING (
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (PARTITION BY {partition_key} ORDER BY {order_key}) AS rn
                        FROM {schema}.{temp_table_name}
                    ) sub
                    WHERE rn = 1
                ) AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values});
            """

            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            snowflake_conn.commit()
            print(f"{temp_table_name} 머지 완료!!!!")


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_SUB_ALLI_01",
    schedule_interval='10 1 * * *',
    start_date=pendulum.datetime(2025, 3, 10, tz="Asia/Seoul"),
    catchup=False,  # 과거 데이터 실행 스킵
    tags=["현대홈쇼핑","DD01_0010_DAILY_MAIN","Scheduled"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )
    @task(task_id='task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I',
          trigger_rule="all_done",
          provide_context=True,
          on_failure_callback=notify_api_on_error)
    def task_AM_ALML_MD_VEN_INTL_SETUP_DTL(table, **kwargs):
        s3_bucket_name = "hdhs-dw-migdata-s3"
        tmp_dir = "/tmp/postgre"

        os.makedirs(tmp_dir, exist_ok=True)

        schema, table_name = table.split('.')

        s3_prefix = f"dw/dms_full_load/{schema}/{table_name}/"

        # 기존 파일 삭제
        delete_existing_files(s3_bucket_name, s3_prefix)

        chunk_index = 1
        while True:
            s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
            offset = (chunk_index - 1) * BATCH_SIZE

            with postgres_hook.get_conn() as conn:
                query = f"""
                        SELECT *
                        FROM {table_name}
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                    """
                df = pd.read_sql(query, conn)

            if df.empty:
                print("No more data to process for this table.")
                break

            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                    else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                )

            df.replace({np.nan: None}, inplace=True)

            file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"
            df.to_parquet(file_name, engine='pyarrow', index=False)
            os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
            os.remove(file_name)

            chunk_index += 1

        with snowflake_hook.get_conn() as conn:
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{table_name}')")
            conn.cursor().execute(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{table_name}')")


    @task(task_id='task_AM_ALML_INTL_EXCP_SETUP_DTL',
          trigger_rule="all_done",
          provide_context=True,
          on_failure_callback=notify_api_on_error)
    def task_AM_ALML_INTL_EXCP_SETUP_DTL(table):
        process_in_batches(table, INTL_EXCP_SETUP_DTL_COLUMNS, INTL_EXCP_SETUP_DTL_pk)

    @task(task_id='task_AM_ALML_ITEM_INTL_DTL',
          trigger_rule="all_done",
          provide_context=True,
          on_failure_callback=notify_api_on_error)
    def task_AM_ALML_ITEM_INTL_DTL(table):
        process_in_batches_sep_hour(table, ITEM_INTL_DTL_COLUMNS, ITEM_INTL_DTL_pk)

    # Task execution
    task_ETL_SCHEDULE_c_01 >> task_AM_ALML_MD_VEN_INTL_SETUP_DTL(TABLE_NAME_LIST[0]) >> task_AM_ALML_INTL_EXCP_SETUP_DTL(TABLE_NAME_LIST[1]) >> task_AM_ALML_ITEM_INTL_DTL(TABLE_NAME_LIST[2])
