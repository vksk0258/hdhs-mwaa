from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from datetime import datetime, timedelta
import pendulum
import boto3
import json
import os


# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

fm_p_start = datetime.strptime(f"{params.get('$$P_START')}000000", "%Y%m%d%H%M%S")
fm_p_end = datetime.strptime(f"{params.get('$$P_END')}235959", "%Y%m%d%H%M%S")

date_folder = fm_p_start.strftime('%Y/%m/%d')
time_identifier = fm_p_end.strftime('%H%M%S')

BATCH_SIZE = 100000

# Constants
S3_BUCKET_NAME = "hdhs-dw-mwaa-migdata"
TMP_DIR = "/tmp/incremental"

table = "DW_RM.RCU_MKTG_AGR_SMS_DTL_TMP"
columns = ['CUST_NO','CUST_NM','TEL','SEND_CNTN','REG_DTM','ETL_DTM']

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

            p_start_add_1d = fm_p_start + timedelta(days=1)
            p_end_add_1d = fm_p_end + timedelta(days=1)

            while True:
                query = f"""
                    SELECT {', '.join(columns)} 
                    FROM {table} 
                """
                query += f"""
                    WHERE REG_DTM >= '{p_start_add_1d}' 
                    AND REG_DTM <= '{p_end_add_1d}'
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


def calculate_params(task_id, p_start, p_end):
    # 1. task_id에서 맨 뒤 _다음 문자 추출
    key_char = task_id.split("_")[-1][0]  # 맨 뒤 _ 다음의 첫 번째 문자

    # 2. from_to_chk 변수 계산
    if key_char == 'F':
        from_to_chk = 'from_to_day'
    elif key_char == 'D':
        from_to_chk = 'day'
    elif key_char == 'M':
        from_to_chk = 'month'
    else:
        raise ValueError("Invalid key_char: Expected 'F', 'D', or 'M'")

    # 3. v_p_dcyl 변수 계산 (첫 번째 문자 이후 숫자 3자리 추출)
    v_p_dcyl = int(task_id.split("_")[-1][1:4])

    # p_start와 p_end 값을 datetime 형식으로 변환
    p_start_date = datetime.strptime(p_start, "%Y%m%d")
    p_end_date = datetime.strptime(p_end, "%Y%m%d")

    # 4. v_p_start 변수 계산
    v_p_start_date = p_start_date - timedelta(days=v_p_dcyl)
    v_p_start = v_p_start_date.strftime("%Y%m%d")

    # 5. v_p_end 변수 계산
    if from_to_chk == 'from_to_day':
        v_p_end = p_end  # 그대로 사용
    else:
        v_p_end_date = p_end_date - timedelta(days=v_p_dcyl)
        v_p_end = v_p_end_date.strftime("%Y%m%d")

    # 6. p_dcyl 변수 계산
    if from_to_chk == 'from_to_day':
        p_dcyl = 0
    else:
        p_dcyl = v_p_dcyl

    # 최종 결과 반환
    return [v_p_start, v_p_end, p_dcyl]

def execute_procedure_dycl(procedure_name, p_start, p_end, **kwargs):
    """
    Executes a stored procedure in Snowflake with calculated parameters.
    """
    task_id = kwargs['task_instance'].task_id
    print(f"Task ID: {task_id}")
    # Calculate parameters based on task_id
    v_p_start, v_p_end, p_dcyl = calculate_params(task_id, p_start, p_end)

    # Snowflake procedure execution
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    start_time = pendulum.now("Asia/Seoul")

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{v_p_start}', '{v_p_end}','{p_dcyl}')"
                print(query)
                # cur.execute(query)
                # result = cur.fetchall()
                # result_message = result[0][0] if result and result[0] else "No result returned"
                # print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")
    end_time = pendulum.now("Asia/Seoul")

    # Log the result to Snowflake
    # log_result_to_snowflake(procedure_name, start_time, end_time, result_message, p_start, p_end)

def execute_procedure(procedure_name, p_start, p_end):
    """
    Executes a stored procedure in Snowflake and logs the result.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    start_time = pendulum.now("Asia/Seoul")

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{p_start}', '{p_end}')"
                print(query)
                # cur.execute(query)
                # result = cur.fetchall()
                # Handle empty result properly
                # result_message = result[0][0] if result and result[0] else "No result returned"
                # print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")
    end_time = pendulum.now("Asia/Seoul")

    # Log the result to Snowflake
    # log_result_to_snowflake(procedure_name, start_time, end_time, result_message, p_start, p_end)


def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")


with DAG(
    dag_id="dag_CDC_MART_MKTG_SMS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_1030_MKTG_AGR_SMS_01"]
) as dag:
    task_SP_BMK_CUST_MKTG_AGR_HIS = PythonOperator(
        task_id="task_SP_RAR_AFCR_ORD_ARLT_SMR",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_AGR_HIS", p_start, p_end],
        trigger_rule="all_done"
    )

    @task(task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01")
    def task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01():
        process_in_batches(table, columns)


    task_SP_CU_MKTG_AGR_SMS_SND_P = PythonOperator(
        task_id="task_SP_CU_MKTG_AGR_SMS_SND_P",
        python_callable=execute_procedure_dycl,
        op_args=["SP_CU_MKTG_AGR_SMS_SND_P", p_start, p_end]
    )

    task_SP_BMK_CUST_MKTG_AGR_HIS >> task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01() >> task_SP_CU_MKTG_AGR_SMS_SND_P