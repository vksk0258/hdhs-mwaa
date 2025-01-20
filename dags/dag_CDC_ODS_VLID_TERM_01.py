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
BCU_CUST_STAT_MST_COLUMNS = ["CUST_NO", "CUST_STAT_GBCD", "NOTC_PRRG_DT", "NOTC_CMPT_DT", "VLID_TERM_EXPY_PRRG_DT", "VLID_TERM_MVOT_CMPT_DT", "DROT_PRRG_DT", "DROT_CMPT_DT", "CUST_REST_DT", "LAST_TRD_DT", "LAST_ACSS_DT", "LAST_CTI_ACSS_DT", "LAST_ONLN_ACSS_DT", "LAST_INSU_CONT_DT", "LAST_DATA_BROD_ORD_DT", "FRST_CUST_REG_DT", "LAST_ORD_DT", "LAST_EVNT_ENTRY_DT", "LAST_SVMT_USE_DT", "LAST_CDPST_USE_DT", "HP_TEL", "HP_VLID_YN", "EMAIL_ADR", "EMAIL_VLID_YN", "ITNT_ID", "ITNT_DROT_YN", "NOTC_MDA_NM", "LAST_DATA_BROD_ACSS_DT", "REG_DTM", "CHG_DTM"]
BMK_CUST_VLID_TERM_EMAIL_DTl_COLUMNS = ["CUST_NO", "EMAIL_ADR", "NOTC_PRRG_DT", "NOTC_CMPT_DT", "VLID_TERM_EXPY_PRRG_DT", "REG_DTM", "CHG_DTM"]


# Constants
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"
TMP_DIR = "/tmp/incremental"
TABLE_NAME_LIST = [
    {"table": "DW_BM.BCU_CUST_STAT_MST", "columns": BCU_CUST_STAT_MST_COLUMNS},
    {"table": "DW_BM.BMK_CUST_VLID_TERM_EMAIL_DTL", "columns": BMK_CUST_VLID_TERM_EMAIL_DTl_COLUMNS}
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

            p_start_add_1d = datetime.datetime.strptime(f"{params.get('$$P_START')}000000",
                                                    "%Y%m%d%H%M%S") + datetime.timedelta(days=1)
            p_end_add_1d = datetime.datetime.strptime(f"{params.get('$$P_END')}235959",
                                                  "%Y%m%d%H%M%S") + datetime.timedelta(days=1)

            while True:
                query = f"""
                    SELECT {', '.join(columns)} 
                    FROM {table} 
                """

                if table == "DW_BM.BCU_CUST_STAT_MST":
                    query += f"""
                        WHERE ETL_DTM BETWEEN '{p_start_add_1d.strftime('%Y-%m-%d %H:%M:%S')}' 
                        AND '{p_end_add_1d.strftime('%Y-%m-%d %H:%M:%S')}'
                        OR VLID_TERM_EXPY_PRRG_DT BETWEEN '{p_start}'
                        AND '{p_end}'
                        LIMIT {BATCH_SIZE} OFFSET {offset}
                    """
                elif table == "DW_BM.BMK_CUST_VLID_TERM_EMAIL_DTL":
                    query += f"""
                        WHERE NOTC_PRRG_DT BETWEEN TO_CHAR(TO_DATE('{p_start}'||'000000', 'YYYYMMDDHH24MISS')+1, 'YYYYMMDD') 
                        AND TO_CHAR(TO_DATE('{p_end}'||'235959', 'YYYYMMDDHH24MISS')+1, 'YYYYMMDD')
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
    dag_id="dag_CDC_ODS_VLID_TERM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    task_SP_TRUNCATE_FOR_DW = PythonOperator(
        task_id="task_SP_TRUNCATE_FOR_DW",
        python_callable=execute_procedure,
        op_args=["SP_TRUNCATE_FOR_DW", p_start, p_end],
        trigger_rule="all_done"
    )
    @task(task_id="task_CU_CUST_STAT_DTL_TO_HDHS_c_01", trigger_rule="all_done")
    def task_CU_CUST_STAT_DTL_TO_HDHS_c_01(table):
        process_in_batches(table['table'], table['columns'])


    @task(task_id="task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_c_01", trigger_rule="all_done")
    def task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_c_01(table):
        process_in_batches(table['table'], table['columns'])


    task_SP_HDHS_CU_CUST_STAT_DTL_DW = PythonOperator(
        task_id="task_SP_HDHS_CU_CUST_STAT_DTL_DW",
        python_callable=execute_procedure,
        op_args=["SP_HDHS_CU_CUST_STAT_DTL_DW", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_TRUNCATE_FOR_DW >> task_CU_CUST_STAT_DTL_TO_HDHS_c_01(TABLE_NAME_LIST[0]) >> \
    [task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_c_01(TABLE_NAME_LIST[1]),task_SP_HDHS_CU_CUST_STAT_DTL_DW]






