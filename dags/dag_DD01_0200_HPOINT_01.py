from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
import pendulum

parent_dir = "100_COM"

with DAG(
    dag_id="dag_DD01_0200_HPOINT_01",
    schedule_interval='30 4 * * *',
    start_date=pendulum.datetime(2025, 2, 10, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    catchup=False,
    tags=[parent_dir,"Scheduled","현대홈쇼핑"]
) as dag:
    trigger_dag_CDC_MART_HPOINT_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_HPOINT_01',
        trigger_dag_id='dag_CDC_MART_HPOINT_01',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_HPOINT_01