from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
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


with DAG(
    dag_id="dag_CDC_MART_LEV_05",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_RIA_PREV_QLTY_EVAL_DLU_SMR = PythonOperator(
        task_id="task_SP_RIA_PREV_QLTY_EVAL_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RIA_PREV_QLTY_EVAL_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RAR_CRDC_SEVT_RST_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_CRDC_SEVT_RST_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_CRDC_SEVT_RST_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_BMK_CUST_MKTG_UTLZ_AGR_INF = PythonOperator(
        task_id="task_SP_BMK_CUST_MKTG_UTLZ_AGR_INF",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_UTLZ_AGR_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RPS_SO_SALE_DLU_DLINE_DTL = PythonOperator(
        task_id="task_SP_RPS_SO_SALE_DLU_DLINE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RPS_SO_SALE_DLU_DLINE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_MSOF_AREA_SO_DMC_CLOG = PythonOperator(
        task_id="task_SP_MSOF_AREA_SO_DMC_CLOG",
        python_callable=execute_procedure,
        op_args=["SP_MSOF_AREA_SO_DMC_CLOG", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RAR_STLM_STAT_DLU_SMR = PythonOperator(
        task_id="task_SP_RAR_STLM_STAT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RAR_STLM_STAT_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCA_DRCS_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_RCA_DRCS_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RCA_DRCS_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_ROD_GRNT_SVMT_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_GRNT_SVMT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_GRNT_SVMT_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_ROD_USE_SVMT_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_USE_SVMT_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_USE_SVMT_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RDM_SO_CH_MOTH_DIM = PythonOperator(
        task_id="task_SP_RDM_SO_CH_MOTH_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_SO_CH_MOTH_DIM", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_ETL_DAILY_LOG = PythonOperator(
        task_id="task_ETL_DAILY_LOG",
        python_callable=log_etl_completion,
        provide_context=True,
        trigger_rule="all_done"
    )


    [task_SP_BMK_CUST_MKTG_UTLZ_AGR_INF, task_SP_RAR_CRDC_SEVT_RST_DLU_SMR, task_SP_RIA_PREV_QLTY_EVAL_DLU_SMR]

    task_SP_RAR_CRDC_SEVT_RST_DLU_SMR >> task_SP_RPS_SO_SALE_DLU_DLINE_DTL >> task_SP_RAR_STLM_STAT_DLU_SMR

    task_SP_RIA_PREV_QLTY_EVAL_DLU_SMR >> task_SP_MSOF_AREA_SO_DMC_CLOG >> task_SP_RCA_DRCS_ARLT_DLU_FCT >> task_SP_ROD_GRNT_SVMT_DLU_SMR >> task_SP_ROD_USE_SVMT_DLU_SMR >> task_SP_RDM_SO_CH_MOTH_DIM

    [task_SP_BMK_CUST_MKTG_UTLZ_AGR_INF, task_SP_RAR_STLM_STAT_DLU_SMR, task_SP_RDM_SO_CH_MOTH_DIM] >> task_ETL_DAILY_LOG

