from airflow import DAG
import datetime
import pendulum
import random
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def snow_chg_wh(wh_size):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as cursor:
            chg_query = f"ALTER WAREHOUSE DW_ETL_WH SET WAREHOUSE_SIZE = '{wh_size}'"
            print(f"수행 쿼리 : {chg_query}")
            cursor.execute(chg_query)
            print(f"수행 로그 : {cursor.fetchone()}")

with DAG(
    dag_id="dags_python_operator",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 왠만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'python']
) as dag:
    snow_chg_wh = PythonOperator(
        task_id="snow_chg_wh",
        python_callable=snow_chg_wh,
        op_args=['2X-LARGE']
    )
    def select_fruit(**kwargs):
        fruit = ['APPLE','BANANA','ORAGNE','AVOCADO']
        #0~3랜덤 정수 값
        rand_int = random.randint(0,3)
        print(fruit[rand_int])
        print(kwargs)

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit
    )

    snow_chg_wh >> py_t1