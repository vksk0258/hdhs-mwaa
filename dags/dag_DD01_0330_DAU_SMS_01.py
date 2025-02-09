from airflow import DAG
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

parent_dir = "100_COM"
parent_dag = "dag_DD01_0330"


with DAG(
    dag_id="dag_DD01_0330_DAU_SMS_01",
    schedule_interval=None,
    tags=[parent_dir, parent_dag, "현대홈쇼핑"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    trigger_dag_CDC_ODS_DAILY_CAMP_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_DAILY_CAMP_01',
        trigger_dag_id='dag_CDC_ODS_DAILY_CAMP_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_ODS_DAU_SMS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_DAU_SMS_01',
        trigger_dag_id='dag_CDC_ODS_DAU_SMS_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    task_ETL_SCHEDULE_c_01 >> trigger_dag_CDC_ODS_DAILY_CAMP_01 >> trigger_dag_CDC_ODS_DAU_SMS_01