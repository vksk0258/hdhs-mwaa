from airflow import DAG
import datetime
import pendulum

from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hdhs_conn_test_ping",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
    ) as dag:
    ping_OCI = BashOperator(
        # 객체명과 task id 는 같게 가는게 좋다

        task_id="ping_OCI",
        bash_command="ping -c 1 10.203.40.108",
    )

    ping_main = BashOperator(
        # 객체명과 task id 는 같게 가는게 좋다
        task_id="ping_main",
        bash_command="ping -c 1 110.4.128.53:1522",
    )

    ping_OCI >> ping_main