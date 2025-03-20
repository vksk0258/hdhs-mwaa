from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException,AirflowFailException
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, execute_procedure_no_dycl, log_etl_completion
from airflow.models import DagRun, TaskInstance
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
from common.notify_error_functions import notify_api_on_error
from datetime import datetime, timedelta
import boto3
import json
import time

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0900_DAILY_BROAD_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

snow_conn_id = 'conn_snowflake_etl'

with (DAG(
    dag_id="dag_CDC_MART_DAILY_BROAD_01",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑","dag_DD01_0630_DAILY_BROAD_01","MART프로시져"]
) as dag):

    task_SP_DW_USE_RATIO = PythonOperator(
        task_id="task_SP_DW_USE_RATIO",
        python_callable=execute_procedure,
        op_args=["SP_DW_USE_RATIO", p_start, p_end, snow_conn_id],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_CTPF_VACO_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_CTPF_VACO_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_CTPF_VACO_DTL", p_start, p_end, snow_conn_id],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_CTPF_VACO_DTL2 = PythonOperator(
        task_id="task_SP_BOD_ORD_CTPF_VACO_DTL2",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_CTPF_VACO_DTL2", p_start, p_end, snow_conn_id],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BOD_ORD_PTC = PythonOperator(
        task_id="task_SP_BOD_ORD_PTC",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_PTC", p_start, p_end, snow_conn_id],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    @task(task_id="fail_dag", trigger_rule="one_failed")
    def fail_dag(**kwargs):
        dag_run: DagRun = kwargs['dag_run']
        task_instances = dag_run.get_task_instances()
        for ti in task_instances:
            if ti.state not in [State.SUCCESS, State.FAILED]:
                ti.set_state(State.FAILED)
        raise Exception("DAG execution failed by custom logic.")

    @task(task_id="if_fail_task", trigger_rule="all_done")
    def if_fail_task(**kwargs):
        ti = kwargs['task_instance']
        status = ti.xcom_pull(key='task_status', task_ids='task_SP_BOD_ORD_PTC')
        if not status:
            raise AirflowSkipException("skipped!!")
        print("success!!")

    task_SP_BOD_ORD_DC_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_DC_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_DC_DTL", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_TV_BITM_ORD_FCT = PythonOperator(
        task_id="task_SP_RIA_TV_BITM_ORD_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_TV_BITM_ORD_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_FCT_01 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_FCT_01", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_SELL_MDA_FCT = PythonOperator(
        task_id="task_SP_RIA_BITM_SELL_MDA_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_SELL_MDA_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_CUST_FCT = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_CUST_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_CUST_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_EXP_ORD_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_D001 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_D001",
        python_callable=execute_procedure_no_dycl,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_02_D001 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_02_D001",
        python_callable=execute_procedure_no_dycl,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT_02", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_D001 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_REAL_FCT_02_D001 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_BITM_ORD_REAL_FCT_02", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_ETC_EXP_ORD_D001= PythonOperator(
        task_id="task_SP_BITM_SELL_ETC_EXP_ORD_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_ETC_EXP_ORD", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_DTBRC_ORD_EXP_FCT_D001 = PythonOperator(
        task_id="task_SP_RIA_DTBRC_ORD_EXP_FCT_D001",
        python_callable=execute_procedure_no_dycl,
        op_args=["SP_RIA_DTBRC_ORD_EXP_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_ETC_EXP_ORD_FOR_SALE_NEWS_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_ETC_EXP_ORD_FOR_SALE_NEWS_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_ETC_EXP_ORD_FOR_SALE_NEWS", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_DTBRC_ORD_EXP_FCT_02_D001 = PythonOperator(
        task_id="task_SP_RIA_DTBRC_ORD_EXP_FCT_02_D001",
        python_callable=execute_procedure_no_dycl,
        op_args=["SP_RIA_DTBRC_ORD_EXP_FCT_02", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_ETC_REAL_ORD_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_ETC_REAL_ORD_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_ETC_REAL_ORD", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_DTBRC_ORD_REAL_FCT_D001 = PythonOperator(
        task_id="task_SP_RIA_DTBRC_ORD_REAL_FCT_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_DTBRC_ORD_REAL_FCT", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_ETC_REAL_ORD_FOR_SALE_NEWS_D001 = PythonOperator(
        task_id="task_SP_BITM_SELL_ETC_REAL_ORD_FOR_SALE_NEWS_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_ETC_REAL_ORD_FOR_SALE_NEWS", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_DTBRC_ORD_REAL_FCT_02_D001 = PythonOperator(
        task_id="task_SP_RIA_DTBRC_ORD_REAL_FCT_02_D001",
        python_callable=execute_procedure_dycl,
        op_args=["SP_RIA_DTBRC_ORD_REAL_FCT_02", p_start, p_end, snow_conn_id],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="none_skipped"
    )

    # @task(task_id="wait_before_execution", trigger_rule="none_skipped")
    # def wait_before_execution():
    #     print("Waiting for 5 minutes before proceeding...")
    #     time.sleep(300)  # 300 seconds = 5 minutes
    #     print("Wait is over. Proceeding to the next task.")
    #
    # task_SP_HDHS_DAILY_DELETE = PythonOperator(
    #     task_id="task_SP_HDHS_DAILY_DELETE",
    #     python_callable=execute_procedure,
    #     op_args=["SP_HDHS_DAILY_DELETE", p_start, p_end],
    #     trigger_rule="none_skipped"
    # )

    trigger_dag_CDC_ODS_DAILY_TO_HDHS = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_DAILY_TO_HDHS',
        trigger_dag_id='dag_CDC_ODS_DAILY_TO_HDHS',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    fail_dag = fail_dag()
    if_fail_task = if_fail_task()

    [task_SP_DW_USE_RATIO, task_SP_BOD_ORD_CTPF_VACO_DTL]

    task_SP_BOD_ORD_CTPF_VACO_DTL >> task_SP_BOD_ORD_CTPF_VACO_DTL2 >> task_SP_BOD_ORD_PTC >> [fail_dag, trigger_dag_CDC_ODS_DAILY_TO_HDHS]

    fail_dag >> if_fail_task >>task_SP_BOD_ORD_DC_DTL >> [task_SP_RIA_TV_BITM_ORD_FCT, task_SP_RIA_BITM_ORD_FCT_01]
    task_SP_RIA_TV_BITM_ORD_FCT >> task_SP_RIA_BITM_SELL_MDA_FCT >> task_SP_RIA_BITM_ORD_CUST_FCT

    task_SP_RIA_BITM_ORD_FCT_01 >> [task_SP_BITM_SELL_EXP_ORD_D001, task_SP_BITM_SELL_ETC_EXP_ORD_D001]

    task_SP_BITM_SELL_EXP_ORD_D001 >> task_SP_RIA_BITM_ORD_EXP_FCT_D001 >> task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_D001 >> task_SP_RIA_BITM_ORD_EXP_FCT_02_D001 >> task_SP_BITM_SELL_REAL_ORD_D001 >> task_SP_RIA_BITM_ORD_REAL_FCT_D001 >> task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_D001 >> task_SP_RIA_BITM_ORD_REAL_FCT_02_D001

    task_SP_BITM_SELL_ETC_EXP_ORD_D001 >> task_SP_RIA_DTBRC_ORD_EXP_FCT_D001 >> task_SP_BITM_SELL_ETC_EXP_ORD_FOR_SALE_NEWS_D001 >> task_SP_RIA_DTBRC_ORD_EXP_FCT_02_D001 >>task_SP_BITM_SELL_ETC_REAL_ORD_D001 >> task_SP_RIA_DTBRC_ORD_REAL_FCT_D001 >> task_SP_BITM_SELL_ETC_REAL_ORD_FOR_SALE_NEWS_D001 >> task_SP_RIA_DTBRC_ORD_REAL_FCT_02_D001 >> task_ETL_DAILY_LOG

    [task_SP_RIA_BITM_ORD_CUST_FCT, task_SP_RIA_BITM_ORD_REAL_FCT_02_D001, task_ETL_DAILY_LOG]

