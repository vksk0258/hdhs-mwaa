from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
from datetime import datetime, timedelta
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
    dag_id="dag_CDC_MART_PGM_REAL_01",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    task_SP_TRUNC_MORF_BITM_SELL_REAL = PythonOperator(
        task_id="task_SP_TRUNC_MORF_BITM_SELL_REAL",
        python_callable=execute_procedure,
        op_args=["SP_TRUNC_MORF_BITM_SELL_REAL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_F015 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_D028 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_D056 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_D084 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_F015 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D028 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D056 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_SELL_REAL_SMR_3_D084 = PythonOperator(
        task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_SELL_REAL_SMR_3", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_EXP_ORD_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_D000 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_02_D000 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_02_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT_02", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_F015 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D028 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D056 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D084 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RDM_ALLI_REF_CH_DIM", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_F015 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D028 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D056 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D084 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084 = PythonOperator(
        task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2", p_start, p_end],
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_TRUNC_MORF_BITM_SELL_REAL >> task_SP_BITM_SELL_REAL_ORD_F015 >> task_SP_BITM_SELL_REAL_ORD_D028 >> task_SP_BITM_SELL_REAL_ORD_D056 >> task_SP_BITM_SELL_REAL_ORD_D084 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_F015 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D028 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D056 >> task_SP_RAR_BITM_SELL_REAL_SMR_3_D084 >> task_SP_BITM_SELL_EXP_ORD_D000 >> task_SP_RIA_BITM_ORD_EXP_FCT_D000 >> task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D000 >> task_SP_RIA_BITM_ORD_EXP_FCT_02_D000 >> task_SP_RIA_BITM_ORD_REAL_FCT_F015 >> task_SP_RIA_BITM_ORD_REAL_FCT_D028 >> task_SP_RIA_BITM_ORD_REAL_FCT_D056 >> task_SP_RIA_BITM_ORD_REAL_FCT_D084 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_F015 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D028 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D056 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D084 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_F015 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D028 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D056 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D084 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_F015 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D028 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D056 >> task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_D084



