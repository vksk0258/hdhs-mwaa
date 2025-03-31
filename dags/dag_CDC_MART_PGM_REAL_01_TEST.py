from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.utils.task_group import TaskGroup
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
    dag_id="dag_CDC_MART_PGM_REAL_01_TEST",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_CDC_MART_01","MART프로시져"]
) as dag:
    task_SP_TRUNC_MORF_BITM_SELL_REAL_TEST = PythonOperator(
        task_id="task_SP_TRUNC_MORF_BITM_SELL_REAL_TEST",
        python_callable=execute_procedure,
        op_args=["SP_TRUNC_MORF_BITM_SELL_REAL_TEST", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    # Group 1
    with TaskGroup("grp_real_ord") as grp_real_ord:
        t1 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_TEST_F015", python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_TEST_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_TEST_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_TEST_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4]

    # Group 2
    with TaskGroup("grp_real_smr_3") as grp_real_smr_3:
        t1 = PythonOperator(task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_TEST_F015",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_SELL_REAL_SMR_3_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_TEST_D028",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_SELL_REAL_SMR_TEST_3", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_TEST_D056",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_SELL_REAL_SMR_3_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_RAR_BITM_SELL_REAL_SMR_3_TEST_D084",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_SELL_REAL_SMR_3_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4]

    # Group 3
    with TaskGroup("grp_exp_fct") as grp_exp_fct:
        t1 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_EXP_FCT_02_TEST_D000", python_callable=execute_procedure,
                            op_args=["SP_RIA_BITM_ORD_EXP_FCT_02_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_TEST_F015", python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_TEST_D028", python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_TEST_D056", python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t5 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_TEST_D084", python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4, t5]

    # Group 4
    with TaskGroup("grp_real_ord_for_sale_news") as grp_real_ord_for_sale_news:
        t1 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST_F015",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST_D028",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST_D056",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST_D084",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_BITM_SELL_REAL_ORD_FOR_SALE_NEWS_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4]

    # Group 5
    with TaskGroup("grp_final_real") as grp_final_real:
        t1 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_TEST_F015",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_02_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t2 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_TEST_D028",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_02_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t3 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_TEST_D056",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_02_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t4 = PythonOperator(task_id="task_SP_RIA_BITM_ORD_REAL_FCT_02_TEST_D084",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RIA_BITM_ORD_REAL_FCT_02_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t5 = PythonOperator(task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST_F015",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t6 = PythonOperator(task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST_D028",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t7 = PythonOperator(task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST_D056",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        t8 = PythonOperator(task_id="task_SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST_D084",
                            python_callable=execute_procedure_dycl,
                            op_args=["SP_RAR_BITM_ORD_PNTM_SELL_REAL_SMR_2_TEST", p_start, p_end, 'conn_snowflake_etl'],
                            provide_context=True, on_failure_callback=notify_api_on_error)
        [t1, t2, t3, t4, t5, t6, t7, t8]

    task_SP_BITM_SELL_EXP_ORD_TEST_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_TEST_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD_TEST", p_start, p_end, 'conn_snowflake_etl'],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RIA_BITM_ORD_EXP_FCT_TEST_D000 = PythonOperator(
        task_id="task_SP_RIA_BITM_ORD_EXP_FCT_TEST_D000",
        python_callable=execute_procedure,
        op_args=["SP_RIA_BITM_ORD_EXP_FCT_TEST", p_start, p_end, 'conn_snowflake_etl'],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_TEST_D000 = PythonOperator(
        task_id="task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_TEST_D000",
        python_callable=execute_procedure_dycl,
        op_args=["SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_TEST", p_start, p_end, 'conn_snowflake_etl'],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_RAR_REAl_SMR_BROD_TEST_01 = PythonOperator(
        task_id="task_SP_RAR_REAl_SMR_BROD_TEST_01",
        python_callable=execute_procedure,
        op_args=["SP_RAR_REAl_SMR_BROD_TEST", p_start, p_end, 'conn_snowflake_etl'],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_TRUNC_MORF_BITM_SELL_REAL_TEST >>\
    grp_real_ord >> grp_real_smr_3 >> \
    task_SP_BITM_SELL_EXP_ORD_TEST_D000 >> task_SP_RIA_BITM_ORD_EXP_FCT_TEST_D000 >> task_SP_BITM_SELL_EXP_ORD_FOR_SALE_NEWS_TEST_D000 >> \
    grp_exp_fct >> grp_real_ord_for_sale_news >> grp_final_real >> \
    task_SP_RAR_REAl_SMR_BROD_TEST_01
