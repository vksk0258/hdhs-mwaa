from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
import pendulum




# DAG 정의
with DAG(
        dag_id="dag_DD01_0010_DAILY_MAIN_01",
        schedule_interval='0 2 * * *',
        start_date=pendulum.datetime(2025, 1, 18, tz="Asia/Seoul"),
        catchup=False,
        tags=["현대홈쇼핑", "Daily"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    # trigger_dag_CDC_MART_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_MART_01',
    #     trigger_dag_id='dag_CDC_MART_01',
    #     trigger_run_id=None,
    #     reset_dag_run=True,
    #     wait_for_completion=False,
    #     poke_interval=60,
    #     allowed_states=['success'],
    #     failed_states=None,
    #     trigger_rule="all_done"
    # )

    trigger_dag_CDC_ODS_SUB_ALLI_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_SUB_ALLI_01',
        trigger_dag_id='dag_CDC_ODS_SUB_ALLI_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=False,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_ODS_SUB_CMS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_SUB_CMS_01',
        trigger_dag_id='dag_CDC_ODS_SUB_CMS_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=False,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )



    task_ETL_SCHEDULE_c_01 >> trigger_dag_CDC_ODS_SUB_ALLI_01 >> trigger_dag_CDC_ODS_SUB_CMS_01
