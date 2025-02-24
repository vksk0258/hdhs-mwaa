from airflow import DAG
from operators.oracle_to_snowflake_merge_operator import OracleToSnowflakeMergeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.decorators import task
from operators.oracle_to_snowflake_initial_load_operator import OracleToSnowflakeInitialLoadOperator
from airflow.models import Variable
import datetime
import pendulum
import boto3
import json

client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

KST = pendulum.timezone("Asia/Seoul")

# 모든 컬럼 리스트
columns = [
    "COCM_CD", "POLY_NO", "TM_CUST_ID", "CNTR_CD", "JOIN_PLNO", "PRT_CD", "CNSR_ID",
    "CMPG_ID", "INSR_GDCD", "INSR_PRDNAME", "INSR_TERM_DVSN", "INSR_TERM",
    "PAYM_TERM_DVSN", "PAYM_TERM", "PAYM_TIMS", "CLLT_USER_ID", "LAST_INSR_MEM_ID",
    "SSRT_DATE", "CNTT_DATE", "CNTT_STMH", "CNTT_DVSN", "CTOR_NAME", "CTOR_RRNO",
    "CTOR_TEL_NO", "CNTT_ZIP_NO", "CNTT_ADDR_DVSN", "CNTT_BASE_ADDR2", "CNTT_ADDR2",
    "INSU_NAME", "INSU_RRNO", "INSU_TEL_NO", "INSU_ZIP_NO", "INSU_ADDR_DVSN",
    "INSU_BASE_ADDR2", "INSU_ADDR2", "COLL_USER_ID", "PAYM_METD_CD", "PAYM_CYCL_CD",
    "JOIN_AMT", "PREM", "CNVS_PREM", "CTST_CHNG_DATE", "CNTT_STAT_CD",
    "CNTT_STAT_DTCD", "LAST_PAYM_MNTH", "EXTN_DATE", "SSRT_WDW_DATE", "WDW_RESN_CD",
    "ALL_APS_YN", "CNTN_CMPG_ID", "CNTN_CLL_USER_ID", "CNTN_CNSL_ID",
    "CNTN_MKTG_RTCD", "CNTN_TCAC_RTCD", "CNTN_TCAC_DTTM", "RGST_DTTM", "RGST_PGM_ID",
    "RGST_ID", "CHNG_ID", "CHNG_DTTM", "CHNG_PGM_ID"
]

# Primary Key 컬럼 리스트
columns_pk = ["COCM_CD", "POLY_NO"]

condition_query = f"""WHERE CHNG_DTTM BETWEEN '{p_start}'||'000000' AND '{p_end}'||'235959'"""

def delete_existing_files(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        print(f"Deleted existing files in S3 path: s3://{bucket_name}/{prefix}")
    else:
        print(f"No existing files to delete in S3 path: s3://{bucket_name}/{prefix}")


# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_INSU_01",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑","TMS", "ODS"]  # DAG에 붙일 태그
) as dag:

    task_TB_CNT_TMST_c_01 = OracleToSnowflakeMergeOperator(
        task_id = "task_TB_CNT_TMST_c_01",
        oracle_conn_id = "conn_oracle_HINS",
        snowflake_conn_id = "conn_snow_load",
        oracle_table = "HINSTM.TB_CNTTMST",
        snowflake_table = "ODS_INSU.TB_CNTTMST",
        columns = columns,
        pk_columns = ['COCM_CD', 'POLY_NO'],
        condition_query = condition_query,
        batch_size = 100000,
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    @task(task_id='task_TB_CU_DORM_CMPL_REST_i_01')
    def task_TB_CU_DORM_CMPL_REST_i_01 ():
        import os
        import pandas as pd
        import numpy as np

        oracle_hook = OracleHook(oracle_conn_id='conn_oracle_HINS', thick_mode=True, thick_mode_lib_dir=client_path)
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        table = 'HINSTM.TB_CU_DORM_CMPL_REST'

        s3_bucket_name = "hdhs-dw-migdata-s3"
        tmp_dir = "/tmp/insu"

        os.makedirs(tmp_dir, exist_ok=True)

        schema, table_name = table.split('.')

        s3_prefix = f"dw/dms_full_load/ODS_INSU/{table_name}/"

        # 기존 파일 삭제
        delete_existing_files(s3_bucket_name, s3_prefix)

        BATCH_SIZE = 200000

        chunk_index = 1
        while True:
            s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
            offset = (chunk_index - 1) * BATCH_SIZE

            with oracle_hook.get_conn() as conn:
                query = f"""
                            SELECT *
                            FROM {table}
                            OFFSET {offset} ROWS FETCH NEXT {BATCH_SIZE} ROWS ONLY
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
            print(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
            os.remove(file_name)

            chunk_index += 1

        with snowflake_hook.get_conn() as conn:
            query = f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', 'ODS_INSU', '{table_name}')"
            print(query)
            conn.cursor().execute(query)


    task_TB_CNT_TMST_c_01 >> task_TB_CU_DORM_CMPL_REST_i_01()
