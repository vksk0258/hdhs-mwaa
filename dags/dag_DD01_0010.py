from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import timedelta
import pendulum

with DAG(
    dag_id="dag_DD01_0010",
    schedule_interval='10 0 * * *',
    start_date=pendulum.datetime(2025, 2, 6, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    tags=["현대홈쇼핑"]
) as dag:
    trigger_dag_DD01_0010_DAILY_MAIN_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_DAILY_MAIN_01',
        trigger_dag_id='dag_DD01_0010_DAILY_MAIN_01',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    # trigger_dag_DD01_0010_MONTHLY_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_DD01_0010_MONTHLY_01',
    #     trigger_dag_id='dag_DD01_0010_MONTHLY_01',
    #     reset_dag_run=True,
    #     wait_for_completion=True,
    #     poke_interval=60,
    #     allowed_states=['success'],
    #     trigger_rule="all_done"
    # )

    trigger_dag_DD01_0010_DAILY_MAIN_01 #>> trigger_dag_DD01_0010_MONTHLY_01