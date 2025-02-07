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
    dag_id="dag_CDC_MART_LEV_04",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_RCU_HS_CUST_MOTH_SMR = PythonOperator(
        task_id="task_SP_RCU_HS_CUST_MOTH_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_HS_CUST_MOTH_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RAR_BFMT_DLU_FCT = PythonOperator(
        task_id="task_SP_RAR_BFMT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_BFMT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_ONLN_ACSS_INF = PythonOperator(
        task_id="task_SP_RCU_CUST_ONLN_ACSS_INF",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_ONLN_ACSS_INF", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_EVNT_PTCP_DLU_SMR = PythonOperator(
        task_id="task_SP_RCU_EVNT_PTCP_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_EVNT_PTCP_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_NEW_ITEM_WKU_SMR = PythonOperator(
        task_id="task_SP_RIA_NEW_ITEM_WKU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RIA_NEW_ITEM_WKU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )
    task_SP_RCA_TMR_CALL_DLU_FCT = PythonOperator(
        task_id="task_SP_RCA_TMR_CALL_DLU_FCTM",
        python_callable=execute_procedure,
        op_args=["SP_RCA_TMR_CALL_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )


    task_SP_RMA_BROD_COPN_USE_DTL = PythonOperator(
        task_id="task_SP_RMA_BROD_COPN_USE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RMA_BROD_COPN_USE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_TV_BITM_ORD_FCT = PythonOperator(
        task_id="task_SP_RIA_TV_BITM_ORD_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_TV_BITM_ORD_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_ORD_CUST_FCT = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_CUST_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_CUST_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RMA_HMALL_COPN_USE_DTL = PythonOperator(
        task_id="task_SP_RMA_HMALL_COPN_USE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RMA_HMALL_COPN_USE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )
    task_SP_ROD_OORD_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_OORD_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_OORD_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_BOD_ORD_STLM_DTL = PythonOperator(
        task_id="task_SP_BOD_ORD_STLM_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BOD_ORD_STLM_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_ROD_VEN_CUST_ORD_DTL = PythonOperator(
        task_id="task_SP_ROD_VEN_CUST_ORD_DTLM",
        python_callable=execute_procedure,
        op_args=["SP_ROD_VEN_CUST_ORD_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_ROD_SO_SALE_DLU_SMR = PythonOperator(
        task_id="task_SP_ROD_SO_SALE_DLU_SMR",
        python_callable=execute_procedure,
        op_args=["SP_ROD_SO_SALE_DLU_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_BBD_BFMT_HOPE_DTLM = PythonOperator(
        task_id="task_SP_BBD_BFMT_HOPE_DTL",
        python_callable=execute_procedure,
        op_args=["SP_BBD_BFMT_HOPE_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_BFMT_PRJ_DTL = PythonOperator(
        task_id="task_SP_RIA_BFMT_PRJ_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BFMT_PRJ_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RDM_MD_ORGN_DIM = PythonOperator(
        task_id="task_SP_RDM_MD_ORGN_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_MD_ORGN_DIM", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RIA_BITM_SELL_MDA_FCTM = PythonOperator(
        task_id="task_SP_RIA_BITM_SELL_MDA_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_SELL_MDA_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done"
    )

    task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL = PythonOperator(
        task_id="task_SP_RCU_CUST_ONLN_ACSS_DLU_DTL",
        python_callable=execute_procedure,
        op_args=["SP_RCU_CUST_ONLN_ACSS_DLU_DTL", p_start, p_end, 'conn_snowflake_etl'],
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
