from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
import pendulum

parent_dir = "100_COM"

with DAG(
    dag_id="dag_HH01_00_ODS_HOURLY_HES_01",
    schedule_interval='0 */2 * * *',
    start_date=pendulum.datetime(2025, 2, 9, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    catchup=False,
    tags=[parent_dir,"Scheduled","현대홈쇼핑"]

) as dag:

    trigger_dag_CDC_ODS_HOURLY_HES_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_HOURLY_HES_01',
        trigger_dag_id='dag_CDC_ODS_HOURLY_HES_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_ODS_HOURLY_HES_01