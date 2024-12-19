from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import datetime
from airflow.models import Variable

client_path = Variable.get("client_path")

table_list = [
    "HDHS_CU.CU_CUST_MST",
    "HDHS_CU.CU_ITNT_CUST_GRD_2_INF",
    "HDHS_CU.CU_ITNT_CUST_GRD_1_INF",
    "HDHS_CU.CU_CUST_INF_AGR_DTL",
    "HDHS_CU.CU_CUST_APP_INF",
    "HDHS_PD.IM_SLITM_MST",
    "HDHS_CM.CM_MD_MST",
    "HDHS_CM.CM_ORGN_MST",
    "HDHS_AM.AM_ALML_MST",
    "HDHS_OD.OD_BASKT_INF",
    "HDHS_CU.CU_SLTD_DTL",
    "HDHS_CU.CU_ONLN_ACSS_LOG",
    "HDHS_BM.BD_MLB_PGM_ALRIM_INF",
    "HDHS_CM.CM_USER_MST",
    "HDHS_DW.CP_CUST_PSN_INF_CHK_MST",
    "HDHS_DW.CP_CUST_SMS_TEL_CHK_MST",
    "HDHS_DW.CP_CUST_DSTN_ADR_CHK_MST",
    "HDHS_DW.AN_PRMO_CUST_DTL",
    "HDHS_OD.OD_ORD_DTL"
]

def setup_connections():
    global oracle_connection, snowflake_connection
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    oracle_connection = oracle_hook.get_conn()
    snowflake_connection = snowflake_hook.get_conn()
    print("Connections initialized.")

# 커넥션 종료
def close_connections():
    global oracle_connection, snowflake_connection
    if oracle_connection:
        oracle_connection.close()
    if snowflake_connection:
        snowflake_connection.close()
    print("Connections closed.")

def oracle_value_extract(table_name):
    global oracle_connection
    if table_name == "HDHS_OD.OD_ORD_DTL":
        ora_query = "select count(*),SUM(LAST_STLM_AMT) from "+table_name
    else:
        ora_query = "select count(*) from "+table_name
    connection = oracle_connection.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(ora_query).fetchall()
    print(result)
    cursor.close()
    connection.close()
    return result

def snow_value_extract(table_name):
    global snowflake_connection
    if table_name == "HDHS_OD.OD_ORD_DTL":
        snow_query = "select count(*),SUM(LAST_STLM_AMT) from DW_LOAD_DB."+table_name
    else:
        snow_query = "select count(*) from DW_LOAD_DB."+table_name
    connection = snowflake_connection.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(snow_query).fetchall()
    print(result)
    cursor.close()
    connection.close()
    return result

def insert_comparison_results(table_name,**kwargs):
    # Snowflake Hook 연결
    global snowflake_connection
    connection = snowflake_connection.get_conn()
    cursor = connection.cursor()

    # DAG 실행 시간에 8시간을 추가
    execution_date = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul"))
    snow_db_name = 'DW_LOAD_DB'
    schema_name = table_name.split(".")[0]
    table = table_name.split(".")[1]

    # XCom에서 값 가져오기
    snow_cnt = int(kwargs['ti'].xcom_pull(task_ids=f'snow_value_extract_{table_name.replace(".", "_")}')[0][0])
    ora_cnt = int(kwargs['ti'].xcom_pull(task_ids=f'oracle_value_extract_{table_name.replace(".", "_")}')[0][0])

    if table_name == "HDHS_OD.OD_ORD_DTL":
        # SUM 값 (필요 시 수정)
        snow_sum = int(kwargs['ti'].xcom_pull(task_ids=f'snow_value_extract_{table_name.replace(".", "_")}')[0][1])
        ora_sum = int(kwargs['ti'].xcom_pull(task_ids=f'oracle_value_extract_{table_name.replace(".", "_")}')[0][1])

        # 차이값 계산
        minus_sum = ora_sum - snow_sum

    else:
        # SUM 값 (필요 시 수정)
        snow_sum = 0
        ora_sum = 0

        # 차이값 계산

        minus_sum = 0

    minus_cnt = ora_cnt - snow_cnt
    DIFF_RANGE = (minus_cnt / ora_cnt) * 100

    # INSERT 쿼리
    insert_query = """
    INSERT INTO DW_LOAD_DB.CONFIG.TB_DATA_VERIFY (
        VERIFY_DATE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, 
        ORA_CNT, SNOW_CNT,
        ORA_SUM, SNOW_SUM,
        MINUS_CNT, MINUS_SUM, DIFF_RANGE
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    parameters = (
        execution_date,
        snow_db_name,
        schema_name,
        table,
        ora_cnt,
        snow_cnt,
        ora_sum,
        snow_sum,
        minus_cnt,
        minus_sum,
        DIFF_RANGE
    )
    cursor.execute(insert_query, parameters)
    connection.commit()
    cursor.close()
    connection.close()


with DAG(
        dag_id="hdhs_value_verifi_daily",
        start_date=pendulum.datetime(2024, 12, 10, tz="Asia/Seoul"),
        schedule_interval="20 * * * *",
        dagrun_timeout=datetime.timedelta(minutes=300),
        catchup=False,
        tags=["현대홈쇼핑"]
) as dag:
    # Task 그룹 저장 리스트
    task_groups = []

    for table in table_list:
        # Oracle 데이터 추출 Task
        oracle_task = PythonOperator(
            task_id=f'oracle_value_extract_{table.replace(".", "_")}',
            python_callable=oracle_value_extract,
            op_args=[table],
            trigger_rule='none_skipped'
        )

        # Snowflake 데이터 추출 Task
        snow_task = PythonOperator(
            task_id=f'snow_value_extract_{table.replace(".", "_")}',
            python_callable=snow_value_extract,
            op_args=[table],
            trigger_rule='none_skipped'
        )

        # 비교 및 결과 삽입 Task
        comparison_task = PythonOperator(
            task_id=f'insert_comparison_results_{table.replace(".", "_")}',
            python_callable=insert_comparison_results,
            op_args=[table],
            trigger_rule='none_skipped'
        )

        # 병렬 작업 후 비교 작업 실행
        [oracle_task, snow_task] >> comparison_task

        # 현재 Task 그룹을 리스트에 추가
        task_groups.append((oracle_task, snow_task, comparison_task))

    # Task 그룹 간 직렬 연결
    for idx in range(len(task_groups) - 1):

        # 이전 그룹의 마지막 Task(비교 작업)를 다음 그룹의 병렬 작업의 첫 Task에 연결
        task_groups[idx][2] >> [task_groups[idx + 1][0], task_groups[idx + 1][1]]