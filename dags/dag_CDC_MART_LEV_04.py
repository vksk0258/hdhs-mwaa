from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
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
    print(f"*** {complete_time} : CDC_MART_LEV_04 프로시져 실행 완료 **")


with DAG(
    dag_id="dag_CDC_MART_LEV_04",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    task_SP_RCU_HS_CUST_MOTH_SMR = PythonOperator(
        task_id="task_SP_RCU_HS_CUST_MOTH_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_HS_CUST_MOTH_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RAR_BFMT_DLU_FCT = PythonOperator(
        task_id="task_SP_RAR_BFMT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BFMT_DLU_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_ONLN_ACSS_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_ONLN_ACSS_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_ONLN_ACSS_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RCU_EVNT_PTCP_DLU_SMR = PythonOperator(
        task_id="task_SP_RCU_EVNT_PTCP_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_EVNT_PTCP_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RIA_NEW_ITEM_WKU_SMR = PythonOperator(
        task_id="task_SP_RIA_NEW_ITEM_WKU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RIA_NEW_ITEM_WKU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )
    task_SP_RCA_TMR_CALL_DLU_FCT = PythonOperator(
        task_id="task_SP_RCA_TMR_CALL_DLU_FCTM",
        python_callable=execute_procedure,
        op_args=["SP_RCA_TMR_CALL_DLU_FCT", p_start, p_end],
        trigger_rule="all_done"
    )


    task_SP_RMA_BROD_COPN_USE_DTL = PythonOperator(
        task_id="task_SP_RMA_BROD_COPN_USE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RMA_BROD_COPN_USE_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RIA_TV_BITM_ORD_FCT = PythonOperator(
        task_id="task_SP_RIA_TV_BITM_ORD_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_TV_BITM_ORD_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_CUST_FCT = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_CUST_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_CUST_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RMA_HMALL_COPN_USE_DTL = PythonOperator(
        task_id="task_SP_RMA_HMALL_COPN_USE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RMA_HMALL_COPN_USE_DTL", p_start, p_end],
        trigger_rule="all_done"
    )
    task_SP_ROD_OORD_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_OORD_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_OORD_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BOD_ORD_STLM_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_STLM_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_STLM_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_ROD_VEN_CUST_ORD_DTL = PythonOperator(
        task_id="task_SP_ROD_VEN_CUST_ORD_DTLM",
        python_callable=execute_procedure,
        op_args=["SP_ROD_VEN_CUST_ORD_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_ROD_SO_SALE_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_SO_SALE_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_SO_SALE_DLU_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BBD_BFMT_HOPE_DTLM = PythonOperator(
        task_id="task_SP_BBD_BFMT_HOPE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BBD_BFMT_HOPE_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RIA_BFMT_PRJ_DTL = PythonOperator(
        task_id="task_SP_RIA_BFMT_PRJ_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BFMT_PRJ_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RDM_MD_ORGN_DIM = PythonOperator(
        task_id="task_SP_RDM_MD_ORGN_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_MD_ORGN_DIM", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_SELL_MDA_FCTM = PythonOperator(
        task_id="task_SP_RIA_BITM_SELL_MDA_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_SELL_MDA_FCT", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL = PythonOperator(
        task_id="task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_ONLN_ACSS_DLU_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )

    [task_SP_RAR_BFMT_DLU_FCT, task_SP_RCU_EVNT_PTCP_DLU_SMR, task_SP_RCU_CUST_ONLN_ACSS_INF, task_SP_RCU_HS_CUST_MOTH_SMR]

    task_SP_RAR_BFMT_DLU_FCT >> task_SP_RCA_TMR_CALL_DLU_FCT >> task_SP_RIA_BITM_ORD_CUST_FCT >> task_SP_ROD_OORD_DLU_SMR

    task_SP_RCU_EVNT_PTCP_DLU_SMR >> task_SP_RMA_BROD_COPN_USE_DTL >> task_SP_RMA_HMALL_COPN_USE_DTL >> task_SP_BOD_ORD_STLM_DTL >> task_SP_ROD_VEN_CUST_ORD_DTL >>task_SP_ROD_SO_SALE_DLU_SMR

    task_SP_RCU_HS_CUST_MOTH_SMR >> task_SP_RIA_NEW_ITEM_WKU_SMR >> task_SP_RIA_TV_BITM_ORD_FCT >> task_SP_BBD_BFMT_HOPE_DTLM >> task_SP_RIA_BFMT_PRJ_DTL >> task_SP_RDM_MD_ORGN_DIM >> task_SP_RIA_BITM_SELL_MDA_FCTM >> task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL

    [task_SP_ROD_OORD_DLU_SMR, task_SP_ROD_SO_SALE_DLU_SMR, task_SP_RCU_CUST_ONLN_ACSS_INF, task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL] >> task_ETL_DAILY_LOG
