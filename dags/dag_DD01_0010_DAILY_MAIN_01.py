from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
import pendulum

parent_dir = "100_COM"
parent_dag = "dag_DD01_0010"

# DAG 정의
with DAG(
        dag_id="dag_DD01_0010_DAILY_MAIN_01",
        schedule_interval=None,
        catchup=False,
        tags=[parent_dir, parent_dag, "현대홈쇼핑"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    # trigger_dag_CDC_ODS_SUB_ALLI_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_SUB_ALLI_01',
    #     trigger_dag_id='dag_CDC_ODS_SUB_ALLI_01',
    #     reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    # trigger_dag_CDC_ODS_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_01',
    #     trigger_dag_id='dag_CDC_ODS_01',
    #     reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    # trigger_dag_CDC_ODS_WEBLOG_CTI_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_WEBLOG_CTI_01',
    #     trigger_dag_id='dag_CDC_ODS_WEBLOG_CTI_01',
    #     reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    trigger_dag_DD01_0010_MKTG_AGR_TERM_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_MKTG_AGR_TERM_01',
        trigger_dag_id='dag_DD01_0010_MKTG_AGR_TERM_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # trigger_dag_CDC_ODS_02 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_02',
    #     trigger_dag_id='dag_CDC_ODS_02',
    #     reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    trigger_dag_DD01_0010_CUST_TNDC_INF_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_CUST_TNDC_INF_01',
        trigger_dag_id='dag_DD01_0010_CUST_TNDC_INF_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_01',
        trigger_dag_id='dag_CDC_MART_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0010_VLID_TERM_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_VLID_TERM_01',
        trigger_dag_id='dag_DD01_0010_VLID_TERM_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0010_PHYS_DIST_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_PHYS_DIST_01',
        trigger_dag_id='dag_DD01_0010_PHYS_DIST_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # trigger_dag_CDC_ODS_SUB_CMS_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_SUB_CMS_01',
    #     trigger_dag_id='dag_CDC_ODS_SUB_CMS_01',
    #     reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )


    # task_ETL_SCHEDULE_c_01 >> [trigger_dag_CDC_ODS_SUB_ALLI_01, trigger_dag_CDC_ODS_01]
    #
    # trigger_dag_CDC_ODS_01 >> [trigger_dag_CDC_ODS_WEBLOG_CTI_01, trigger_dag_DD01_0010_MKTG_AGR_TERM_01, trigger_dag_CDC_ODS_02, trigger_dag_DD01_0010_CUST_TNDC_INF_01, trigger_dag_CDC_MART_01]
    #
    # trigger_dag_CDC_ODS_WEBLOG_CTI_01 >> trigger_dag_DD01_0010_VLID_TERM_01
    #
    # trigger_dag_CDC_MART_01 >> trigger_dag_DD01_0010_PHYS_DIST_01


    task_ETL_SCHEDULE_c_01 >> [ trigger_dag_DD01_0010_MKTG_AGR_TERM_01, trigger_dag_DD01_0010_CUST_TNDC_INF_01, trigger_dag_CDC_MART_01, trigger_dag_DD01_0010_VLID_TERM_01]

    trigger_dag_CDC_MART_01 >> trigger_dag_DD01_0010_PHYS_DIST_01
