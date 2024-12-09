from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.models import Variable
import pandas as pd
import datetime
import pprint
import time

snow_conn = Variable.get("snow_conn")
snow_query = Variable.get("query")
def load_data():
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snow_conn)
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(snow_query)
    print("====================================================================")
    for i in cursor.fetchall():
        print(i)
    connection.close()
    cursor.close()

with DAG(
    dag_id="hdhs_snow_load_test",
    schedule_interval=None,
    default_args={"snowflake_conn_id": snow_conn},
    dagrun_timeout=datetime.timedelta(minutes=300),
    tags=["현대홈쇼핑"]
) as dag:
    load_data_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data
    )

    snow_task = SnowflakeSqlApiOperator(
        task_id='snow_task',
        sql=snow_query
    )

    load_data_task >> snow_task