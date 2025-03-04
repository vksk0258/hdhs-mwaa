from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.models import Variable
import pendulum

parent_dir = "100_COM"

snow_wh = Variable.get('1_batch_wh')

def snow_chg_wh(wh_size):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as cursor:
            chg_query = f"ALTER WAREHOUSE DW_ETL_WH SET WAREHOUSE_SIZE = '{wh_size}'"
            print(f"수행 쿼리 : {chg_query}")
            cursor.execute(chg_query)
            print(f"수행 로그 : {cursor.fetchone()}")

with DAG(
    dag_id="dag_DD01_0010",
    schedule_interval='0 2 * * *',
    start_date=pendulum.datetime(2025, 2, 10, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    catchup=False,
    tags=[parent_dir, "Scheduled","현대홈쇼핑"]
) as dag:
    snow_chg_wh = PythonOperator(
        task_id="snow_chg_wh",
        python_callable=snow_chg_wh,
        op_args=[snow_wh]
    )

    trigger_dag_DD01_0010_DAILY_MAIN_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_DAILY_MAIN_01',
        trigger_dag_id='dag_DD01_0010_DAILY_MAIN_01',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0200_HPOINT_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0200_HPOINT_01',
        trigger_dag_id='dag_DD01_0200_HPOINT_01',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0010_MONTHLY_01 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0010_MONTHLY_01',
        trigger_dag_id='dag_DD01_0010_MONTHLY_01',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
        allowed_states=['success'],
        trigger_rule="all_done"
    )

    snow_chg_wh >> trigger_dag_DD01_0010_DAILY_MAIN_01 >> [trigger_dag_DD01_0010_MONTHLY_01 , trigger_dag_DD01_0200_HPOINT_01]