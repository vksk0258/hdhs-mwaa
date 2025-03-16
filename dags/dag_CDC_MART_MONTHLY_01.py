from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from common.notify_error_functions import notify_api_on_error
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_MONTHLY_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")


with DAG(
    dag_id="dag_CDC_MART_MONTHLY_01",
    schedule_interval=None,
    catchup=False,
    tags=["현대홈쇼핑", "MART프로시져"]
) as dag:
    @task.branch(task_id='branching',
        provide_context=True,
        on_failure_callback=notify_api_on_error)
    def check_monthly(p_end):
        if p_end[6:8] == '01': # 7번째(인덱스 6)부터 2글자
            return ['task_SP_ROD_CUST_PET_GRD_INF', 'task_SP_RCU_HMALL_CUST_GRD_DTL']
        elif (datetime.strptime(p_end, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")[6:8] == '01':
            return ['task_SP_DAU_SMS_DELETE', 'task_SP_COPY_STTC', 'task_SP_RPD_DPRCH_ITEM_MOTH_STCK_INF']
        elif p_end[6:8] == '02':
            return 'task_SP_PCU_CUST_ANAL_FCT_UPDT'
        elif p_end[6:8] == '03':
            return 'task_SP_RCU_MOTH_MDA_CUST_D3_SMR'
        elif p_end[6:8] == '14':
            return 'task_SP_RCU_MOTH_MDA_CUST_SMR'

    task_SP_ROD_CUST_PET_GRD_INF = PythonOperator(
        task_id="task_SP_ROD_CUST_PET_GRD_INF",
        python_callable=execute_procedure,
        op_args=["SP_ROD_CUST_PET_GRD_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_HMALL_CUST_GRD_DTL = PythonOperator(
        task_id="task_SP_RCU_HMALL_CUST_GRD_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RCU_HMALL_CUST_GRD_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_GGC_MOTH_SMR = PythonOperator(
        task_id="SP_RCU_GGC_MOTH_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_GGC_MOTH_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BCU_CUST_MST_UPDATE = PythonOperator(
        task_id="task_SP_BCU_CUST_MST_UPDATE",
        python_callable=execute_procedure,
        op_args=["SP_BCU_CUST_MST_UPDATE", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PCU_CUST_AGR_MOTH_FCT_02 = PythonOperator(
        task_id="task_SP_PCU_CUST_AGR_MOTH_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_AGR_MOTH_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PCU_CUST_AGR_MOTH_FCT_03 = PythonOperator(
        task_id="task_SP_PCU_CUST_AGR_MOTH_FCT_03",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_AGR_MOTH_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_CRM_MOTH_KPI_FCT = PythonOperator(
        task_id="task_SP_RCU_CRM_MOTH_KPI_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CRM_MOTH_KPI_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_CUST_BUY_CHRTR_MOTH_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_BUY_CHRTR_MOTH_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_BUY_CHRTR_MOTH_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_COPY_STTC = PythonOperator(
        task_id="task_SP_COPY_STTC",
        python_callable=execute_procedure,
        op_args=["SP_COPY_STTC", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_CTPF_RATE_DTL = PythonOperator(
        task_id="task_SP_RAR_CTPF_RATE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_CTPF_RATE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_CTPF_RATE_ETC_DTL = PythonOperator(
        task_id="task_SP_RAR_CTPF_RATE_ETC_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_CTPF_RATE_ETC_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_CTPF_RATE_HMALL_DTL = PythonOperator(
        task_id="task_SP_RAR_CTPF_RATE_HMALL_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_CTPF_RATE_HMALL_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BRND_CTPF_RATE_DTL = PythonOperator(
        task_id="task_SP_RAR_BRND_CTPF_RATE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BRND_CTPF_RATE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_BRND_CTPF_RATE_ETC_DTL = PythonOperator(
        task_id="task_SP_RAR_BRND_CTPF_RATE_ETC_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BRND_CTPF_RATE_ETC_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RPS_DRCT_PRCH_LOSS_DTL = PythonOperator(
        task_id="task_SP_RPS_DRCT_PRCH_LOSS_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RPS_DRCT_PRCH_LOSS_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_EXP_SWRT_HMALL_DTL = PythonOperator(
        task_id="task_SP_RAR_EXP_SWRT_HMALL_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RAR_EXP_SWRT_HMALL_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_VEN_CNTB_RATE_SMR = PythonOperator(
        task_id="task_SP_RAR_VEN_CNTB_RATE_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_VEN_CNTB_RATE_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RMA_WINT_INSM_CMISR_INF = PythonOperator(
        task_id="task_SP_RMA_WINT_INSM_CMISR_INF",
        python_callable=execute_procedure,
        op_args=["SP_RMA_WINT_INSM_CMISR_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RPD_DPRCH_ITEM_MOTH_STCK_INF = PythonOperator(
        task_id="task_SP_RPD_DPRCH_ITEM_MOTH_STCK_INF",
        python_callable=execute_procedure,
        op_args=["SP_RPD_DPRCH_ITEM_MOTH_STCK_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_KWRD_PGM_ACHV_RATE_SMR = PythonOperator(
        task_id="task_SP_RIA_KWRD_PGM_ACHV_RATE_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RIA_KWRD_PGM_ACHV_RATE_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_MOTH_MDA_CUST_D3_SMR = PythonOperator(
        task_id="task_SP_RCU_MOTH_MDA_CUST_D3_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_MOTH_MDA_CUST_D3_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PCU_CUST_ANAL_FCT_UPDT = PythonOperator(
        task_id="task_SP_PCU_CUST_ANAL_FCT_UPDT",
        python_callable=execute_procedure,
        op_args=["SP_PCU_CUST_ANAL_FCT_UPDT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_MOTH_MDA_CUST_SMR = PythonOperator(
        task_id="task_SP_RCU_MOTH_MDA_CUST_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_MOTH_MDA_CUST_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_ACCLN_SALE_DTL = PythonOperator(
        task_id="task_SP_ACCLN_SALE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_ACCLN_SALE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    # 해당 프로시저는 통합테스트때 테스트
    task_SP_DAU_SMS_DELETE = PythonOperator(
        task_id="task_SP_DAU_SMS_DELETE",
        python_callable=execute_procedure,
        op_args=["SP_DAU_SMS_DELETE", p_start, p_end, 'conn_snowflake_etl'], # 비활성화
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_HDHS_INSU_DELETE = PythonOperator(
        task_id="task_SP_HDHS_INSU_DELETE",
        python_callable=execute_procedure,
        op_args=["SP_HDHS_INSU_DELETE", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    check_monthly(p_end) >> [task_SP_ROD_CUST_PET_GRD_INF, task_SP_RCU_HMALL_CUST_GRD_DTL, task_SP_DAU_SMS_DELETE, \
    task_SP_COPY_STTC, task_SP_RPD_DPRCH_ITEM_MOTH_STCK_INF, task_SP_RCU_MOTH_MDA_CUST_D3_SMR, task_SP_RCU_MOTH_MDA_CUST_SMR, task_SP_PCU_CUST_ANAL_FCT_UPDT]

    task_SP_RCU_HMALL_CUST_GRD_DTL >> task_SP_RCU_GGC_MOTH_SMR >> task_SP_BCU_CUST_MST_UPDATE >> task_SP_PCU_CUST_AGR_MOTH_FCT_02 >> task_SP_PCU_CUST_AGR_MOTH_FCT_03

    [task_SP_ROD_CUST_PET_GRD_INF, task_SP_PCU_CUST_AGR_MOTH_FCT_03] >> task_SP_RCU_CRM_MOTH_KPI_FCT >> \
    task_SP_RCU_CUST_BUY_CHRTR_MOTH_INF

    task_SP_DAU_SMS_DELETE >> task_SP_HDHS_INSU_DELETE

    task_SP_COPY_STTC >> task_SP_RAR_CTPF_RATE_DTL >> task_SP_RAR_CTPF_RATE_ETC_DTL >> task_SP_RAR_CTPF_RATE_HMALL_DTL >> \
    task_SP_RAR_BRND_CTPF_RATE_DTL >> task_SP_RAR_BRND_CTPF_RATE_ETC_DTL >> task_SP_RPS_DRCT_PRCH_LOSS_DTL >> \
    task_SP_RAR_EXP_SWRT_HMALL_DTL >> task_SP_RAR_VEN_CNTB_RATE_SMR >> task_SP_RMA_WINT_INSM_CMISR_INF

    task_SP_RPD_DPRCH_ITEM_MOTH_STCK_INF >> task_SP_RIA_KWRD_PGM_ACHV_RATE_SMR

    task_SP_RCU_MOTH_MDA_CUST_SMR >> task_SP_ACCLN_SALE_DTL













