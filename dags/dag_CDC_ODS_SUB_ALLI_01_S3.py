from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import datetime
import pandas as pd
import boto3
import json
import os

# Column definitions
MD_VEN_INTL_SETUP_DTL_COLUMNS = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
ITEM_INTL_DTL_COLUMNS = ["ALML_CD", "SLITM_CD", "ALML_ITEM_CD", "ALML_CO_GBCD", "ALML_INTL_NO", "SOON_USE_PRMO_NO", "SOON_USE_PRMO_PRC", "ADD_DC_PRMO_NO", "ADD_DC_PRMO_PRC", "SELL_PRC", "ALML_SELL_GBCD", "SELL_GBCD", "ITNT_DISP_YN", "ITEM_PRC_APLY_DTM", "VEN_CD", "VEN2_CD", "OSHP_VEN_ADR_SEQ", "RTP_EXCH_VEN_ADR_SEQ", "SDLVC_VEN_SEQ", "DLVC_PAY_GBCD", "BNDL_DLVC_GBCD", "NCHG_DLV_BSIC_AMT", "DLVC_BSIC_QTY", "DLV_COST", "RTP_DLV_COST", "EXCH_DLV_COST", "SEND_DTM", "ALML_INTL_RST_GBCD", "ALML_ERR_CD", "ALML_ERR_MSG", "ORGL_ALML_ITEM_CD", "ALML_APRVL_STAT_CD", "ALML_PRC_APRVL_STAT_CD", "RJT_PTC_RSN", "RPROC_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
INTL_EXCP_SETUP_DTL_COLUMNS = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "ITEM_INTL_GBCD", "ITEM_INTL_PTC_CD", "RMRK", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]

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

fm_p_start = datetime.datetime.strptime(p_start, "%Y%m%d%H%M%S")
fm_p_end = datetime.datetime.strptime(p_end, "%Y%m%d%H%M%S")

date_folder = fm_p_start.strftime('%Y/%m/%d')
time_identifier = fm_p_end.strftime('%H%M%S')

postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hdhs_reading')
snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

BATCH_SIZE = 500000


def process_in_batches(table, query, schema, table_name):
    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    try:
        with postgres_hook.get_conn() as postgres_conn:
            offset = 0
            batch_number = 1
            while True:
                batch_query = f"""{query} LIMIT {BATCH_SIZE} OFFSET {offset};"""
                df = pd.read_sql(batch_query, postgres_conn)
                if df.empty:
                    break

                file_name = f"{TMP_DIR}/{table_name}_batch{batch_number}_{fm_p_end.strftime('%Y%m%d')}_{time_identifier}.parquet"
                s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{fm_p_end.strftime('%Y%m%d')}-batch{batch_number}-{time_identifier}.parquet"

                df.to_parquet(file_name, engine='pyarrow', index=False)
                print(f"Data batch {batch_number} saved to {file_name}")

                os.system(f"aws s3 cp {file_name} {s3_path}")
                print(f"Uploaded {file_name} to {s3_path}")

                os.remove(file_name)
                print(f"Deleted {file_name} from local directory")

                offset += BATCH_SIZE
                batch_number += 1

    except Exception as e:
        print(f"Error processing table {table}: {e}")

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_SUB_ALLI_01_S3",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑"]
) as dag:
    @task(task_id='task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I')
    def task_AM_ALML_MD_VEN_INTL_SETUP_DTL(table):
        if not os.path.exists(TMP_DIR):
            os.makedirs(TMP_DIR)

        schema, table_name = table.split('.')

        try:
            with postgres_hook.get_conn() as postgres_conn:
                query = f"SELECT * FROM {table_name}"
                df = pd.read_sql(query, postgres_conn)

            if df.empty:
                print(f"No data to process for table {table} in the given time window.")
                return

            file_name = f"{TMP_DIR}/{table_name}_{fm_p_end.strftime('%Y%m%d')}_{time_identifier}.parquet"
            s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{fm_p_end.strftime('%Y%m%d')}-{time_identifier}.parquet"

            df.to_parquet(file_name, engine='pyarrow', index=False)
            print(f"Data saved to {file_name}")

            os.system(f"aws s3 cp {file_name} {s3_path}")
            print(f"Uploaded {file_name} to {s3_path}")

            os.remove(file_name)
            print(f"Deleted {file_name} from local directory")

        except Exception as e:
            print(f"Error processing table {table}: {e}")

    @task(task_id='task_AM_ALML_INTL_EXCP_SETUP_DTL')
    def task_AM_ALML_INTL_EXCP_SETUP_DTL(table):
        schema, table_name = table.split('.')
        query = f"""
                    SELECT 'I' AS Op, 
                           t.*, 
                           t.CHG_DTM AS transact_id
                    FROM {table_name} t
                    WHERE t.REG_DTM = t.CHG_DTM
                      AND t.CHG_DTM >= TO_TIMESTAMP('{fm_p_start}', 'YYYY-MM-DD HH24:MI:SS')
                      AND t.CHG_DTM < TO_TIMESTAMP('{fm_p_end}', 'YYYY-MM-DD HH24:MI:SS')
                    UNION ALL
                    SELECT 'U' AS Op, 
                           t.*, 
                           t.CHG_DTM AS transact_id
                    FROM {table_name} t
                    WHERE t.REG_DTM <> t.CHG_DTM
                      AND t.CHG_DTM >= TO_TIMESTAMP('{fm_p_start}', 'YYYY-MM-DD HH24:MI:SS')
                      AND t.CHG_DTM < TO_TIMESTAMP('{fm_p_end}', 'YYYY-MM-DD HH24:MI:SS')
                """
        print(query)
        process_in_batches(table, query, schema, table_name)

    @task(task_id='task_AM_ALML_ITEM_INTL_DTL')
    def task_AM_ALML_ITEM_INTL_DTL(table):
        schema, table_name = table.split('.')
        query = f"""
                    SELECT 'I' AS Op, 
                           t.*, 
                           t.CHG_DTM AS transact_id
                    FROM {table_name} t
                    WHERE t.REG_DTM = t.CHG_DTM
                      AND t.CHG_DTM >= TO_TIMESTAMP('{fm_p_start}', 'YYYY-MM-DD HH24:MI:SS')
                      AND t.CHG_DTM < TO_TIMESTAMP('{fm_p_end}', 'YYYY-MM-DD HH24:MI:SS')
                    UNION ALL
                    SELECT 'U' AS Op, 
                           t.*, 
                           t.CHG_DTM AS transact_id
                    FROM {table_name} t
                    WHERE t.REG_DTM <> t.CHG_DTM
                      AND t.CHG_DTM >= TO_TIMESTAMP('{fm_p_start}', 'YYYY-MM-DD HH24:MI:SS')
                      AND t.CHG_DTM < TO_TIMESTAMP('{fm_p_end}', 'YYYY-MM-DD HH24:MI:SS')
                """
        process_in_batches(table, query, schema, table_name)

    # Task execution
    task_AM_ALML_MD_VEN_INTL_SETUP_DTL(TABLE_NAME_LIST[0]) >> task_AM_ALML_INTL_EXCP_SETUP_DTL(TABLE_NAME_LIST[1]) >> task_AM_ALML_ITEM_INTL_DTL(TABLE_NAME_LIST[2])
