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

# Define the task function
def execute_procedure(procedure_name, p_start, p_end):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')

    with snowflake_hook.get_conn() as conn:
        with conn.cursor() as cur:
            query = f"CALL ETL_SERVICE.{procedure_name}('{p_start}', '{p_end}')"
            print(query)
            cur.execute(query)
            result = cur.fetchall()
            print(f"Procedure result: {result[0]}")

def log_etl_completion(**kwargs):
    complete_time = kwargs['execution_date'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')
    print(f"*** {complete_time} : CDC_MART_LEV_02 프로시져 실행 완료 **")

# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_LEV_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    task_SP_RDM_ALLI_REF_CH_DIM = PythonOperator(
        task_id="task_SP_RDM_ALLI_REF_CH_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_ALLI_REF_CH_DIM", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCU_CUST_REFL_CTEL_INF = PythonOperator(
        task_id="task_SP_BCU_CUST_REFL_CTEL_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_REFL_CTEL_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_RDM_SELL_MDA_DIM = PythonOperator(
        task_id="task_SP_RDM_SELL_MDA_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_SELL_MDA_DIM", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCU_SMS_RCV_AGR_CUST_INF = PythonOperator(
        task_id="task_SP_BCU_SMS_RCV_AGR_CUST_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_SMS_RCV_AGR_CUST_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCU_PUSH_RCV_AGR_CUST_INF = PythonOperator(
        task_id="task_SP_BCU_PUSH_RCV_AGR_CUST_INF",
        python_callable=execute_procedure,
        op_args=["SP_BCU_PUSH_RCV_AGR_CUST_INF", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCU_EMAIL_ADR_REFI_CUST_MST = PythonOperator(
        task_id="task_SP_BCU_EMAIL_ADR_REFI_CUST_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_EMAIL_ADR_REFI_CUST_MST", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BCU_CUST_MST = PythonOperator(
        task_id="task_SP_BCU_CUST_MST",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_MST", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BOD_ORD_CTPF_VACO_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_CTPF_VACO_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_CTPF_VACO_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BOD_ORD_PTC = PythonOperator(
        task_id="task_SP_BOD_ORD_PTC",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_PTC", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BIM_ITEM_MST = PythonOperator(
        task_id="task_SP_BIM_ITEM_MST",
        python_callable=execute_procedure,
        op_args=["SP_BIM_ITEM_MST", p_start, p_end],
        trigger_rule="all_done"
    )

    task_SP_BOD_ORD_DC_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_DC_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_DC_DTL", p_start, p_end],
        trigger_rule="all_done"
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )


    [task_SP_RDM_ALLI_REF_CH_DIM, task_SP_BCU_CUST_REFL_CTEL_INF, task_SP_RDM_SELL_MDA_DIM]

    task_SP_BCU_CUST_REFL_CTEL_INF >> [task_SP_BCU_SMS_RCV_AGR_CUST_INF, task_SP_BCU_EMAIL_ADR_REFI_CUST_MST]

    task_SP_BCU_SMS_RCV_AGR_CUST_INF >> task_SP_BCU_PUSH_RCV_AGR_CUST_INF

    [task_SP_BCU_PUSH_RCV_AGR_CUST_INF, task_SP_BCU_EMAIL_ADR_REFI_CUST_MST] >> task_SP_BCU_CUST_MST

    task_SP_RDM_SELL_MDA_DIM >> [task_SP_BOD_ORD_CTPF_VACO_DTL, task_SP_BIM_ITEM_MST]

    task_SP_BOD_ORD_CTPF_VACO_DTL >> task_SP_BOD_ORD_PTC >> task_SP_BOD_ORD_DC_DTL

    [task_SP_RDM_ALLI_REF_CH_DIM, task_SP_BCU_CUST_MST, task_SP_BOD_ORD_DC_DTL, task_SP_BIM_ITEM_MST] >> task_ETL_DAILY_LOG


