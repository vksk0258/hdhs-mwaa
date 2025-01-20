from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import boto3
import json

# S3 parameters
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
    print(f"*** {complete_time} : CDC_MART_LEV_05 프로시져 실행 완료 **")

with DAG(
    dag_id="dag_CDC_MART_MANG_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    task_SP_RAR_BMNG_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_RAR_BMNG_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BMNG_ARLT_DLU_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BOD_RNTL_ORD_DTL = PythonOperator(
        task_id="task_SP_BOD_RNTL_ORD_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_RNTL_ORD_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_BMNG_ARLT_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_BMNG_ARLT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BMNG_ARLT_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_HMALL_BMNG_ARLT_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_HMALL_BMNG_ARLT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_HMALL_BMNG_ARLT_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_HS_BMNG_ARLT_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_HS_BMNG_ARLT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_HS_BMNG_ARLT_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_SRCN_MD_ARLT_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_SRCN_MD_ARLT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_SRCN_MD_ARLT_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT = PythonOperator(
        task_id="task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT_02 = PythonOperator(
        task_id="task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT_02", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_01 = PythonOperator(
        task_id="task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_01", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_02 = PythonOperator(
        task_id="task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_02", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_03 = PythonOperator(
        task_id="task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_03",
        python_callable=execute_procedure,
        op_args=["SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_03", p_start, p_end],
        trigger_rule="all_done"
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BMNG_ARLT_DLU_FCT >> [task_SP_BOD_RNTL_ORD_DTL, task_SP_RAR_BMNG_ARLT_DLU_SMR]

    task_SP_RAR_BMNG_ARLT_DLU_SMR >> task_SP_RAR_HMALL_BMNG_ARLT_DLU_SMR >> task_SP_RAR_HS_BMNG_ARLT_DLU_SMR >> task_SP_RAR_SRCN_MD_ARLT_DLU_SMR >> task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT >> task_SP_RAR_BMNG_ARLT_DLU_ACPT_META_FCT_02 >> task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_01 >> task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_02 >> task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_03

    [task_SP_BOD_RNTL_ORD_DTL, task_SP_POD_ACPT_META_ORD_ANAL_DLU_FCT_03] >> task_ETL_DAILY_LOG


