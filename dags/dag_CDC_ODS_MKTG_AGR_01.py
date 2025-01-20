from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import datetime
import pandas as pd
import boto3
import json
import os

# Column definitions
BMK_CUST_MKTG_AGR_MST_COLUMNS = [
    "CUST_NO", "CUST_STAT_GBCD", "CUST_NM", "NOTC_PRRG_DT", "NOTC_CMPT_DT",
    "MKTG_AGR_RFS_YN", "MKTG_AGR_RFS_DT", "INSU_OB_RCV_AGR_YN", "INSU_OB_RCV_AGR_DT",
    "EMAIL_RCV_AGR_YN", "EMAIL_RCV_AGR_DT", "SMS_RCV_AGR_YN", "SMS_RCV_AGR_DT",
    "PUSH_RCV_AGR_YN", "PUSH_RCV_AGR_DT", "MKTG_RFS_TRGT_YN", "RGST_ID",
    "REG_DTM", "CHG_DTM"
]
BMK_CUST_MKTG_AGR_EMAIL_DTL_COLUMNS = [
    "CUST_NO", "CUST_NM", "EMAIL_ADR", "NOTC_PRRG_DT", "NOTC_CMPT_DT",
    "MKTG_AGR_RFS_DT", "INSU_OB_RCV_AGR_DT", "EMAIL_RCV_AGR_DT", "SMS_RCV_AGR_DT",
    "PUSH_RCV_AGR_DT", "RGST_ID", "REG_DTM", "CHG_DTM"
]

# Constants
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"
TMP_DIR = "/tmp/incremental"
TABLE_NAME_LIST = [
    {"table": "DW_BM.BMK_CUST_MKTG_AGR_MST", "columns": BMK_CUST_MKTG_AGR_MST_COLUMNS},
    {"table": "DW_BM.BMK_CUST_MKTG_AGR_EMAIL_DTL", "columns": BMK_CUST_MKTG_AGR_EMAIL_DTL_COLUMNS}
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

BATCH_SIZE = 100000


def process_in_batches(table, columns):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')

    if not os.path.exists(TMP_DIR):
        os.makedirs(TMP_DIR)

    schema, table_name = table.split('.')
    print(table)

    try:
        with snowflake_hook.get_conn() as conn:
            start_date = (fm_p_start + datetime.timedelta(days=1)).strftime('%Y-%m-%d')
            end_date = (fm_p_end + datetime.timedelta(days=1)).strftime('%Y-%m-%d')

            offset = 0
            batch_number = 1

            while True:
                query = f"""
                    SELECT {', '.join(columns)} 
                    FROM {table} 
                """

                if table == "DW_BM.BMK_CUST_MKTG_AGR_MST":
                    query += f"""
                        WHERE MKTG_RFS_TRGT_YN = 'Y' 
                        AND NOTC_PRRG_DT >= '{start_date}' 
                        AND NOTC_PRRG_DT <= '{end_date}' 
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                    """
                elif table == "DW_BM.BMK_CUST_MKTG_AGR_EMAIL_DTL":
                    query += f"""
                        WHERE NOTC_PRRG_DT >= '{start_date}' 
                        AND NOTC_PRRG_DT <= '{end_date}' 
                        AND ETL_DTM >= '{fm_p_start}'
                        AND ETL_DTM <= '{fm_p_end}'
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                    """

                print(query)

                df = pd.read_sql(query, conn)
                if df.empty:
                    break

                file_name = f"{TMP_DIR}/{table_name}_batch{batch_number}_{fm_p_end.strftime('%Y%m%d')}_{time_identifier}.parquet"
                s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{fm_p_end.strftime('%Y%m%d')}-batch{batch_number}-{time_identifier}.parquet"

                df.to_parquet(file_name, engine='pyarrow', index=False)
                os.system(f"aws s3 cp {file_name} {s3_path}")
                os.remove(file_name)

                offset += BATCH_SIZE
                batch_number += 1

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")


# Define the DAG
with DAG(
        dag_id="dag_CDC_ODS_MKTG_AGR_01",
        schedule_interval=None,
        catchup=False,
        tags=["현대홈쇼핑", "dag_DD01_0030_MKTG_AGR_TERM"]
) as dag:
    previous_task = None

    # Create a task for each table in a serial manner
    for table in TABLE_NAME_LIST:
        def create_task(table):
            @task(task_id=f"task_{table['table'].replace('.', '_')}_TO_HDHS_c_01")
            def process_table_task():
                process_in_batches(table['table'], table['columns'])

            return process_table_task


        current_task = create_task(table)()

        # Chain tasks in a serial order
        if previous_task:
            previous_task >> current_task

        previous_task = current_task
