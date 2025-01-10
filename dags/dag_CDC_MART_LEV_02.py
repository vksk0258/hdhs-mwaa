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

# Define the task function
def execute_procedure(procedure_name, p_start, p_end):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            query = f"CALL ETL_SERVICE.{procedure_name}({p_start}, {p_end})"
            print(query)

def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")

with DAG(
    dag_id="dag_CDC_MART_LEV_02",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑","MART 구성","프로시져"]
) as dag:

    task_SP_RCU_CUST_MOTH_ORD_SMR = PythonOperator(
        task_id="task_SP_RCU_CUST_MOTH_ORD_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_MOTH_ORD_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_MDA_ORD_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_MDA_ORD_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_MDA_ORD_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL = PythonOperator(
        task_id="task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCM_HMALL_SECT_DPTH_MST = PythonOperator(
        task_id="task_SP_BCM_HMALL_SECT_DPTH_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCM_HMALL_SECT_DPTH_MST", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_ROD_AREA_ORD_SMR = PythonOperator(
        task_id="task_SP_ROD_AREA_ORD_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_AREA_ORD_SMR", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCM_POST_NO_SI_DO_INF = PythonOperator(
        task_id="task_SP_BCM_POST_NO_SI_DO_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCM_POST_NO_SI_DO_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_MDA_ORD_INF >> task_SP_ROD_AREA_ORD_SMR >> task_SP_BCM_POST_NO_SI_DO_INF

    [task_SP_RCU_CUST_MOTH_ORD_SMR, task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL, task_SP_BCM_HMALL_SECT_DPTH_MST,task_SP_BCM_POST_NO_SI_DO_INF] >> task_ETL_DAILY_LOG