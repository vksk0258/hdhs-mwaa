from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
from datetime import timedelta
import pendulum

parent_dir = "100_COM"

with DAG(
    dag_id="dag_DD01_0300_DAILY_MORD_PGM_01",
    schedule_interval=None,
    # start_date=pendulum.datetime(2025, 2, 10, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    catchup=False,
    tags=[parent_dir,"Scheduled","현대홈쇼핑"]
) as dag:
    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    trigger_dag_CDC_MART_MORD_PGM_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_MORD_PGM_01',
        trigger_dag_id='dag_CDC_MART_MORD_PGM_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0330 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0330',
        trigger_dag_id='dag_DD01_0330',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    task_ETL_SCHEDULE_c_01 >> trigger_dag_CDC_MART_MORD_PGM_01 >> trigger_dag_DD01_0330

