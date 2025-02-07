from airflow import DAG
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dag_DD01_0330_CMS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","DD01_0010_DAILY_MAIN"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    trigger_dag_CDC_MART_CMS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_CMS_01',
        trigger_dag_id='dag_CDC_MART_CMS_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # # 정방향
    # trigger_dag_CDC_ODS_CMS_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_CMS_01',
    #     trigger_dag_id='dag_CDC_ODS_CMS_01',
    #     reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    task_ETL_SCHEDULE_c_01 >> trigger_dag_CDC_MART_CMS_01