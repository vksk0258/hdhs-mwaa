from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd
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



with DAG(
    dag_id="dag_CDC_MART_MKTG_SMS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_1030_MKTG_AGR_SMS_01", "MART프로시져"]
) as dag:
    task_SP_BMK_CUST_MKTG_AGR_HIS = PythonOperator(
        task_id="task_SP_BMK_CUST_MKTG_AGR_HIS",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_AGR_HIS", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    # @task(task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01")
    # def task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01():
    #     process_in_batches(table, columns)
    #
    #
    # task_SP_CU_MKTG_AGR_SMS_SND_P = PythonOperator(
    #     task_id="task_SP_CU_MKTG_AGR_SMS_SND_P",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_CU_MKTG_AGR_SMS_SND_P", p_start, p_end]
    #)

    task_SP_BMK_CUST_MKTG_AGR_HIS # >> task_RAR_REAL_SWRT_DTL_TO_HDHS_c_01() >> task_SP_CU_MKTG_AGR_SMS_SND_P