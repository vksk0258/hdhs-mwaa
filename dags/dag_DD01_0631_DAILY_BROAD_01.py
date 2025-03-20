from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from operators.etl_schedule_update_operator import etlScheduleUpdateOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.models import Variable
import pendulum

parent_dir = "100_COM"

snow_wh = Variable.get('2_batch_wh')

def snow_chg_wh(wh_size):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as cursor:
            chg_query = f"ALTER WAREHOUSE DW_ETL_WH SET WAREHOUSE_SIZE = '{wh_size}'"
            print(f"수행 쿼리 : {chg_query}")
            cursor.execute(chg_query)
            print(f"수행 로그 : {cursor.fetchone()}")

with DAG(
    dag_id="dag_DD01_0631_DAILY_BROAD_01",
    schedule_interval='10 7 * * *',
    start_date=pendulum.datetime(2025, 2, 20, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    catchup=False,
    tags=[parent_dir,"Scheduled","현대홈쇼핑"]
) as dag:
    snow_chg_wh = PythonOperator(
        task_id="snow_chg_wh",
        python_callable=snow_chg_wh,
        op_args=[snow_wh]
    )

    task_ETL_SCHEDULE_c_01 = etlScheduleUpdateOperator(
        task_id="task_ETL_SCHEDULE_c_01"
    )

    trigger_dag_CDC_MART_DAILY_ARLT_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DAILY_ARLT_01',
        trigger_dag_id='dag_CDC_MART_DAILY_ARLT_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DAILY_BROAD_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DAILY_BROAD_01',
        trigger_dag_id='dag_CDC_MART_DAILY_BROAD_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_DAILY_DRCT_DASH_BOARD_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DAILY_DRCT_DASH_BOARD_01',
        trigger_dag_id='dag_CDC_MART_DAILY_DRCT_DASH_BOARD_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # # 역방향
    # trigger_dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01_v2 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01_v2',
    #     trigger_dag_id='dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01_v2',
    #     reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
    #     wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
    #     poke_interval=60,
    #     allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
    #     trigger_rule="all_done"
    # )

    trigger_dag_CDC_MART_DAILY_PGM_FCT_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_DAILY_PGM_FCT_01',
        trigger_dag_id='dag_CDC_MART_DAILY_PGM_FCT_01',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_DD01_0710_ON_DEMAND_02 = TriggerDagRunOperator(
        task_id='trigger_dag_DD01_0710_ON_DEMAND_02',
        trigger_dag_id='dag_DD01_0710_ON_DEMAND_02',
        reset_dag_run=True,  # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'],  # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    # task_ETL_SCHEDULE_c_01 >> [trigger_dag_CDC_MART_DAILY_ARLT_01,trigger_dag_CDC_MART_DAILY_BRAOD_01]
    #
    # trigger_dag_CDC_MART_DAILY_BRAOD_01 >> trigger_dag_CDC_MART_DAILY_DRCT_DASH_BROAD_01
    #
    # trigger_dag_CDC_MART_DAILY_ARLT_01 >> trigger_dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01
    #
    # [trigger_dag_CDC_MART_DAILY_DRCT_DASH_BROAD_01, trigger_dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01] >> \
    # trigger_dag_CDC_MART_DAILY_PGM_FCT_01

    snow_chg_wh >> task_ETL_SCHEDULE_c_01 >> [trigger_dag_CDC_MART_DAILY_ARLT_01,trigger_dag_CDC_MART_DAILY_BROAD_01]

    trigger_dag_CDC_MART_DAILY_BROAD_01 >> trigger_dag_CDC_MART_DAILY_DRCT_DASH_BOARD_01

    trigger_dag_CDC_MART_DAILY_DRCT_DASH_BOARD_01 >> \
    trigger_dag_CDC_MART_DAILY_PGM_FCT_01 >> trigger_dag_DD01_0710_ON_DEMAND_02
