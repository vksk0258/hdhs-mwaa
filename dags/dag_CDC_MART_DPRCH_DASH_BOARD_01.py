from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0900_DAILY_BROAD_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")


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
    INSERT INTO DW_ETL_DB.DW_ETC.JOB_RESULT_MWAA (
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
                query = f"CALL ETL_SERVICE.{procedure_name}('{p_start}', '{p_end}')"
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
    log_result_to_snowflake(procedure_name, start_time, end_time, result_message, p_start, p_end)


def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")



with DAG(
    dag_id="dag_CDC_MART_DPRCH_DASH_BOARD_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0800_DPRCH_DASH_BOARD_01"]
) as dag:
    task_SP_RPS_DPRCH_IO_DTL = PythonOperator(
        task_id="task_SP_RPS_DPRCH_IO_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RPS_DPRCH_IO_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RPS_DPRCH_SCO_DTL = PythonOperator(
        task_id="task_SP_RPS_DPRCH_SCO_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RPS_DPRCH_SCO_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RPS_DPRCH_SIS_DTL = PythonOperator(
        task_id="task_SP_RPS_DPRCH_SIS_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RPS_DPRCH_SIS_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_ROD_DPRCH_ORD_DTL = PythonOperator(
        task_id="task_SP_ROD_DPRCH_ORD_DTL",
        python_callable=execute_procedure,
        op_args=["SP_ROD_DPRCH_ORD_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_ROD_DPRCH_BROD_ORD_DTL = PythonOperator(
        task_id="task_SP_ROD_DPRCH_BROD_ORD_DTL",
        python_callable=execute_procedure,
        op_args=["SP_ROD_DPRCH_BROD_ORD_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    [task_SP_RPS_DPRCH_IO_DTL, task_SP_RPS_DPRCH_SCO_DTL]
    task_SP_RPS_DPRCH_IO_DTL >> task_SP_ROD_DPRCH_ORD_DTL >> task_SP_ROD_DPRCH_BROD_ORD_DTL
    task_SP_RPS_DPRCH_SCO_DTL >> task_SP_RPS_DPRCH_SIS_DTL

