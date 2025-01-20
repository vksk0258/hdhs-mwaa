from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import datetime
import pandas as pd
import pendulum
import boto3
import json
import os

# Column definitions
BCU_CUST_TNDC_INF_COLUMNS = columns = [
    "CUST_NO", "ORD_CNT", "REF_ACPT_CNT", "REF_ACPT_RATE", "STPT_CMPLN_CNT",
    "STPT_CMPLN_RATE", "CNCL_RTP_CNT", "CNCL_RTP_RATE", "ITEM_EVAL_AVG_SCRG", "ITEM_EVAL_CNT",
    "INTG_ITEM_CNSL_ACPT_CNT", "RULE_BESI_RTP_CNT", "INSU_BESI_ORD_CNT", "RGST_ID", "RGST_IP",
    "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"
]

# Constants
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"
TMP_DIR = "/tmp/incremental"
TABLE_NAME_LIST = [
    {"table": "DW_BM.BCU_CUST_TNDC_INF", "columns": BCU_CUST_TNDC_INF_COLUMNS}
]

# Load parameters from S3
s3 = boto3.client('s3')
PARAM_BUCKET_NAME = "hdhs-dw-mwaa-s3"
PARAM_KEY = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=PARAM_BUCKET_NAME, Key=PARAM_KEY)
params = json.load(response['Body'])

# Parse time parameters
p_start = params.get('$$P_START')
p_end = params.get('$$P_END')

fm_p_start = datetime.datetime.strptime(f"{params.get('$$P_START')}000000", "%Y%m%d%H%M%S")
fm_p_end = datetime.datetime.strptime(f"{params.get('$$P_END')}235959", "%Y%m%d%H%M%S")

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
            offset = 0
            batch_number = 1


            while True:
                query = f"""
                    SELECT {', '.join(columns)} 
                    FROM {table} 
                """

                query += f"""
                    WHERE SMR_DT BETWEEN '{p_start}'
                    AND '{p_end}'
                    LIMIT {BATCH_SIZE} OFFSET {offset}
                """

                print(query)

                df = pd.read_sql(query, conn)
                if df.empty:
                    break

                file_name = f"{TMP_DIR}/{table_name}_batch{batch_number}_{fm_p_end.strftime('%Y%m%d')}_{time_identifier}.parquet"
                s3_path = f"s3://{S3_BUCKET_NAME}/dw/{schema}/{table_name}/{date_folder}/{fm_p_end.strftime('%Y%m%d')}-batch{batch_number}-{time_identifier}.parquet"

                for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                    df[col] = df[col].apply(
                        lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                        else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                    )

                df.to_parquet(file_name, engine='pyarrow', index=False)
                os.system(f"aws s3 cp {file_name} {s3_path}")
                os.remove(file_name)

                offset += BATCH_SIZE
                batch_number += 1

    except Exception as e:
        print(f"Error processing table {table_name}: {e}")


def log_result_to_snowflake(procedure_name, start_time, end_time, result, p_start, p_end):
    """
    Logs the result of a procedure execution to Snowflake table.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    # Ensure result is not None
    result = result or "No result returned"

    # Check for errors in result
    if 'SQL compilation error' in result or 'Procedure execute error' in result:
        status = 'ER'
    else:
        status = 'OK'

    message = result.replace("'", "''")
    jb_pmt = f'[{p_start}]-[{p_end}]'

    query = f"""
    INSERT INTO DW_ETL_DB.CONFIG.JOB_RESULT (
        PGMID, STARTTIME, ENDTIME, ST, JBPMT, MSG
    )
    VALUES (
        '{procedure_name}', '{start_time}', '{end_time}', '{status}', '{jb_pmt}', '{message}'
    )
    """
    print(query)

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def execute_procedure(procedure_name, p_start, p_end):
    """
    Executes a stored procedure in Snowflake and logs the result.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    start_time = pendulum.now("Asia/Seoul")

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL HDHS_NSTD.{procedure_name}('{p_start}', '{p_end}')"
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                # Handle empty result properly
                result_message = result[0][0] if result and result[0] else "No result returned"
                print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")
    end_time = pendulum.now("Asia/Seoul")

    # Log the result to Snowflake
    # log_result_to_snowflake(procedure_name, start_time, end_time, result_message, p_start, p_end)


def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_CUST_TNDC_INF_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    @task(task_id="task_BCU_CUST_TNDC_INF_TO_HDHS_c_01", trigger_rule="all_done")
    def task_BCU_CUST_TNDC_INF_TO_HDHS_c_01(table):
        process_in_batches(table['table'], table['columns'])


    task_BCU_CUST_TNDC_INF_TO_HDHS_c_01(TABLE_NAME_LIST[0])








