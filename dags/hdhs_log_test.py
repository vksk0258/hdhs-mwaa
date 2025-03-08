from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
# from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from airflow.models import Variable  # Airflow 변수 관리
import pendulum
import boto3
import json

# Oracle Client 라이브러리 경로를 변수에서 가져옴
client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

columns = [
                "PGMID", "STARTTIME", "ENDTIME", "ST", "JBPMT",
                "READCNT", "BYCNT", "ERRCNT", "UPDCNT", "WRTCNT",
                "MSG"
            ]

ora_columns = [
                "PRG_NM", "STRT_DTM", "END_DTM", "EXEC_RST_VAL", "PARA_VAL",
                "EXEC_CNT", "SKIP_CNT", "ERR_CNT", "UPDT_CNT", "WRT_CNT",
                "MSG"
            ]

def log_result_to_snowflake(procedure_name, start_time, end_time, p_start, p_end):
    """
    Logs the result of a procedure execution to Snowflake table.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)


    start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
    end_time = end_time.strftime('%Y-%m-%d %H:%M:%S')

    select_query = f"""
                    SELECT {', '.join(columns)} FROM DW_ETL_DB.DW_ETC.JOB_RESULT WHERE PGMID = '{procedure_name}'
                    AND STARTTIME >= TO_TIMESTAMP_NTZ('{start_time}', 'YYYY-MM-DD HH24:MI:SS')
                    AND ENDTIME <= TO_TIMESTAMP_NTZ('{end_time}', 'YYYY-MM-DD HH24:MI:SS')
                    ORDER BY STARTTIME DESC
                    LIMIT 1
                    """

    print("SELECT QUERY")
    print(select_query)

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute(select_query)
            latest_row = cur.fetchone()

    print(latest_row)

    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cur:
            msg = latest_row[10] if latest_row[10] is None else latest_row[10].replace("'", "''")

            insert_query = f"""
                            INSERT INTO HDHS_DW.DW_EXEC_RST ({", ".join(ora_columns)})
                            VALUES (
                            '{latest_row[0]}',
                            TO_TIMESTAMP('{latest_row[1]}','YYYY-MM-DD HH24:MI:SS'),
                            TO_TIMESTAMP('{latest_row[2]}','YYYY-MM-DD HH24:MI:SS'),
                            '{latest_row[3]}',
                            '{latest_row[4]}',
                            {latest_row[5]},
                            {latest_row[6]},
                            {latest_row[7]},
                            {latest_row[8]},
                            {latest_row[9]},
                            '{msg}'
                            )
                        """
            print("INSERT QUERY")
            print(insert_query)

            cur.execute(insert_query)


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

def execute_procedure_dycl(procedure_name, p_start, p_end, conn_id, **kwargs):
    """
    Executes a stored procedure in Snowflake with calculated parameters.
    """
    task_id = kwargs['task_instance'].task_id
    print(f"Task ID: {task_id}")
    # Calculate parameters based on task_id
    v_p_start, v_p_end, p_dcyl = calculate_params(task_id, p_start, p_end)

    # Snowflake procedure execution
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
    start_time = pendulum.now("Asia/Seoul")

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{v_p_start}', '{v_p_end}',{p_dcyl})"
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                result_message = result[0][0] if result and result[0] else "No result returned"
                print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")
    end_time = pendulum.now("Asia/Seoul")

    # Log the result to Snowflake
    log_result_to_snowflake(procedure_name, start_time, end_time, p_start, p_end)

def execute_procedure_no_dycl(procedure_name, p_start, p_end, conn_id, **kwargs):
    """
    Executes a stored procedure in Snowflake with calculated parameters.
    """
    task_id = kwargs['task_instance'].task_id
    print(f"Task ID: {task_id}")
    # Calculate parameters based on task_id
    v_p_start, v_p_end, p_dcyl = calculate_params(task_id, p_start, p_end)

    # Snowflake procedure execution
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
    start_time = pendulum.now("Asia/Seoul")

    try:
        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                query = f"CALL ETL_SERVICE.{procedure_name}('{v_p_start}', '{v_p_end}')"
                print(query)
                cur.execute(query)
                result = cur.fetchall()
                result_message = result[0][0] if result and result[0] else "No result returned"
                print(f"Procedure result: {result_message}")
    except Exception as e:
        result_message = str(e)
        print(f"Procedure execute error message: {result_message}")
    end_time = pendulum.now("Asia/Seoul")

    # Log the result to Snowflake
    log_result_to_snowflake(procedure_name, start_time, end_time, p_start, p_end)


def execute_procedure(procedure_name, p_start, p_end,conn_id,):
    """
    Executes a stored procedure in Snowflake and logs the result.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=conn_id)
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
    log_result_to_snowflake(procedure_name, start_time, end_time, p_start, p_end)


# Define the DAG
with DAG(
    dag_id="hdhs_log_test",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0010_MKTG_AGR_TERM","테스트"]
) as dag:

    task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01 = PythonOperator(
        task_id="task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01_2 = PythonOperator(
        task_id="task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01_2",
        python_callable=execute_procedure,
        op_args=["SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01", p_start, '231', 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    # task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01 = PythonOperator(
    #     task_id="task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01",
    #     python_callable=execute_procedure,
    #     op_args=["SP_BMK_CUST_MKTG_AGR_EMAIL_DTL", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )

    [task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01,task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01_2]