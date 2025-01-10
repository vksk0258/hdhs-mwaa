from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0400_CMS_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

def log_result_to_snowflake(procedure_name, start_time, result, p_start, p_end):
    """
    Logs the result of a procedure execution to Snowflake table.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    status = 'OK' if 'SQL compilation error' not in result else 'ER'
    message = result
    jb_pmt = f'[{p_start}]-[{p_end}]'

    query = f"""
    INSERT INTO DW_ETL_DB.CONFIG.JOB_RESULT (
        PGMID, STARTTIME, ENDTIME, ST, JBPMT, MSG
    )
    VALUES (
        '{procedure_name}', '{start_time}', CURRENT_TIMESTAMP, '{status}', '{jb_pmt}', '{message}'
    )
    """

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def execute_procedure(procedure_name, p_start, p_end, **kwargs):
    """
    Executes a stored procedure in Snowflake and logs the result.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    start_time = pendulum.now().to_iso8601_string()

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{p_start}', '{p_end}')"
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                result_message = result[0][0] if result else "Unknown result"
                print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)

    # Log the result to Snowflake
    log_result_to_snowflake(procedure_name, start_time, result_message, p_start, p_end)


with DAG(
    dag_id="dag_CDC_MART_ON_DEMAND_03",
    schedule_interval=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["현대홈쇼핑", "MART프로시져"]
) as dag:

    task_SP_RAR_AFCR_ORD_ARLT_SMR = PythonOperator(
        task_id="task_SP_RAR_AFCR_ORD_ARLT_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_AFCR_ORD_ARLT_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_SCWD_DLU_ORD_ARLT_SMR = PythonOperator(
        task_id="task_SP_RAR_SCWD_DLU_ORD_ARLT_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_SCWD_DLU_ORD_ARLT_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_DPTS_ITEM_REG_SMR = PythonOperator(
        task_id="task_SP_RAR_DPTS_ITEM_REG_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_DPTS_ITEM_REG_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_AFCR_ORD_ARLT_SMR >> task_SP_RAR_SCWD_DLU_ORD_ARLT_SMR >> task_SP_RAR_DPTS_ITEM_REG_SMR
