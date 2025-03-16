from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from datetime import datetime, timedelta
from common.notify_error_functions import notify_api_on_error
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
    dag_id="dag_CDC_MART_DAILY_PGM_FCT_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0630_DAILY_BROAD_01","MART프로시져"]
) as dag:
    task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015 = PythonOperator(
        task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    # task_SP_PIA_BROD_ANAL_DLU_FCT_02_D028 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D028",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PIA_BROD_ANAL_DLU_FCT_02_D056 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D056",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PIA_BROD_ANAL_DLU_FCT_02_D084 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_02_D084",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )


    task_SP_PIA_BROD_ANAL_DLU_FCT_01_F015 = PythonOperator(
        task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    # task_SP_PIA_BROD_ANAL_DLU_FCT_01_D028 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D028",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    #
    # task_SP_PIA_BROD_ANAL_DLU_FCT_01_D056 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D056",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    #
    # task_SP_PIA_BROD_ANAL_DLU_FCT_01_D084 = PythonOperator(
    #     task_id="task_SP_PIA_BROD_ANAL_DLU_FCT_01_D084",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PIA_BROD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )


    task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_F015 = PythonOperator(
        task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_F015 = PythonOperator(
        task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_F015",
        python_callable=execute_procedure_dycl,
        op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D028 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D028",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D028 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D028",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D056 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D056",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D056 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D056",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    #
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D084 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D084",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )
    #
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D084 = PythonOperator(
    #     task_id="task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D084",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
    #     trigger_rule="all_done"
    # )


    # task_SP_TOBE영업전략일반 = PythonOperator(
    #     task_id="task_SP_TOBE영업전략일반",
    #     python_callable=execute_procedure,
    #     op_args=["SP_TOBE영업전략일반", p_start, p_end],
    #     trigger_rule="all_done"
    # )
    #
    #
    # task_SP_TOBE_HMALL일반 = PythonOperator(
    #     task_id="task_SP_TOBE_HMALL일반",
    #     python_callable=execute_procedure,
    #     op_args=["SP_TOBE_HMALL일반", p_start, p_end],
    #     trigger_rule="all_done"
    # )


    task_SP_RAR_PGM_BUY_CUST_FCT = PythonOperator(
        task_id="task_SP_RAR_PGM_BUY_CUST_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_PGM_BUY_CUST_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RDM_BITM_GRP_DIM = PythonOperator(
        task_id="task_SP_RDM_BITM_GRP_DIM",
        python_callable=execute_procedure,
        op_args=["SP_RDM_BITM_GRP_DIM", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RCU_BITM_DT_ARLT_SMR = PythonOperator(
        task_id="task_SP_RCU_BITM_DT_ARLT_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_BITM_DT_ARLT_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_RCU_BITM_CNSL_SMR = PythonOperator(
        task_id="task_SP_RCU_BITM_CNSL_SMR",
        python_callable=execute_procedure,
        op_args=["SP_RCU_BITM_CNSL_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_POD_ORD_ANAL_DLU_FCT_01 = PythonOperator(
        task_id="task_SP_POD_ORD_ANAL_DLU_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_POD_ORD_ANAL_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_POD_ORD_ANAL_DLU_FCT_03 = PythonOperator(
        task_id="task_SP_POD_ORD_ANAL_DLU_FCT_03",
        python_callable=execute_procedure,
        op_args=["SP_POD_ORD_ANAL_DLU_FCT_03", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_RAR_PGM_UITM_FCT = PythonOperator(
        task_id="task_SP_RAR_PGM_UITM_FCT",
        python_callable=execute_procedure,
        op_args=["SP_RAR_PGM_UITM_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PAR_RNTL_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_PAR_RNTL_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PAR_RNTL_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_PAR_PHDS_ARLT_DLU_FCT = PythonOperator(
        task_id="task_SP_PAR_PHDS_ARLT_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_PAR_PHDS_ARLT_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01 = PythonOperator(
        task_id="task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01",
        python_callable=execute_procedure,
        op_args=["SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_02 = PythonOperator(
        task_id="task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PAR_SHPL_ORD_ARLT_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_POD_ACPT_ORD_ANAL_DLU_FCT = PythonOperator(
        task_id="task_SP_POD_ACPT_ORD_ANAL_DLU_FCT",
        python_callable=execute_procedure,
        op_args=["SP_POD_ACPT_ORD_ANAL_DLU_FCT", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_PAR_HMALL_DLU_ARLT_SMR = PythonOperator(
        task_id="task_SP_PAR_HMALL_DLU_ARLT_SMR",
        python_callable=execute_procedure,
        op_args=["SP_PAR_HMALL_DLU_ARLT_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_POD_ACPT_DLU_ORD_SMR = PythonOperator(
        task_id="task_SP_POD_ACPT_DLU_ORD_SMR",
        python_callable=execute_procedure,
        op_args=["SP_POD_ACPT_DLU_ORD_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_DLV_WTHDW_CNT_SMRL = PythonOperator(
        task_id="task_SP_DLV_WTHDW_CNT_SMRL",
        python_callable=execute_procedure,
        op_args=["SP_DLV_WTHDW_CNT_SMR", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_PAR_PHDS_ARLT_DLU_FCT_02 = PythonOperator(
        task_id="task_SP_PAR_PHDS_ARLT_DLU_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PAR_PHDS_ARLT_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_DLV_OSHP_WTDW_DTL = PythonOperator(
        task_id="task_SP_DLV_OSHP_WTDW_DTL",
        python_callable=execute_procedure,
        op_args=["SP_DLV_OSHP_WTDW_DTL", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_SP_PAR_RNTL_ARLT_DLU_FCT_02 = PythonOperator(
        task_id="task_SP_PAR_RNTL_ARLT_DLU_FCT_02",
        python_callable=execute_procedure,
        op_args=["SP_PAR_RNTL_ARLT_DLU_FCT_02", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    [task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015, task_SP_RAR_PGM_BUY_CUST_FCT]

    task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015 >> \
    task_SP_PIA_BROD_ANAL_DLU_FCT_01_F015 >> \
    task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_F015 >> task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_F015

    task_SP_RAR_PGM_BUY_CUST_FCT >> task_SP_RDM_BITM_GRP_DIM >> \
    task_SP_RCU_BITM_DT_ARLT_SMR >> task_SP_RCU_BITM_CNSL_SMR >> \
    task_SP_POD_ORD_ANAL_DLU_FCT_01 >> task_SP_POD_ORD_ANAL_DLU_FCT_03 >> task_SP_RAR_PGM_UITM_FCT >> \
    task_SP_PAR_RNTL_ARLT_DLU_FCT >> task_SP_PAR_PHDS_ARLT_DLU_FCT >> \
    task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_01 >> task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT_02 >> task_SP_PAR_HMALL_DLU_ARLT_SMR >> \
    task_SP_POD_ACPT_DLU_ORD_SMR >> task_SP_POD_ACPT_ORD_ANAL_DLU_FCT >> \
    task_SP_DLV_WTHDW_CNT_SMRL >> [task_SP_PAR_PHDS_ARLT_DLU_FCT_02, task_SP_DLV_OSHP_WTDW_DTL]

    task_SP_DLV_OSHP_WTDW_DTL >> task_SP_PAR_RNTL_ARLT_DLU_FCT_02

    # [task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015, task_SP_RAR_PGM_BUY_CUST_FCT]
    #
    # task_SP_PIA_BROD_ANAL_DLU_FCT_02_F015 >> task_SP_PIA_BROD_ANAL_DLU_FCT_02_D028 >> \
    # task_SP_PIA_BROD_ANAL_DLU_FCT_02_D056 >> task_SP_PIA_BROD_ANAL_DLU_FCT_02_D084 >> \
    # task_SP_PIA_BROD_ANAL_DLU_FCT_01_F015 >> task_SP_PIA_BROD_ANAL_DLU_FCT_01_D028 >> \
    # task_SP_PIA_BROD_ANAL_DLU_FCT_01_D056 >> task_SP_PIA_BROD_ANAL_DLU_FCT_01_D084 >> \
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_F015 >> task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_F015 >> \
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D028 >> task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D028 >> \
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D056 >> task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D056 >> \
    # task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_D084 >> task_SP_PAR_SHPL_BROD_ARLT_DLU_FCT_01_D084
    #
    # task_SP_RAR_PGM_BUY_CUST_FCT >> task_SP_RDM_BITM_GRP_DIM >> \
    # task_SP_RCU_BITM_DT_ARLT_SMR >> task_SP_RCU_BITM_CNSL_SMR >> \
    # task_SP_POD_ORD_ANAL_DLU_FCT_01 >> task_SP_POD_ORD_ANAL_DLU_FCT_03 >> task_SP_RAR_PGM_UITM_FCT >> \
    # task_SP_PAR_RNTL_ARLT_DLU_FCT >> task_SP_PAR_PHDS_ARLT_DLU_FCT >> \
    # task_SP_PAR_SHPL_ORD_ARLT_DLU_FCT >> task_SP_PAR_HMALL_DLU_ARLT_SMR >> \
    # task_SP_POD_ACPT_DLU_ORD_SMR >> task_SP_POD_ACPT_ORD_ANAL_DLU_FCT >> \
    # task_SP_DLV_WTHDW_CNT_SMRL >> [task_SP_PAR_PHDS_ARLT_DLU_FCT_02, task_SP_DLV_OSHP_WTDW_DTL]
    #
    # task_SP_DLV_OSHP_WTDW_DTL >> task_SP_PAR_RNTL_ARLT_DLU_FCT_02










