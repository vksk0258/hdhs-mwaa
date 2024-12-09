from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

def oracle_conn_test():
    postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hshs_reading')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    print(cursor)
    cursor.close()
    connection.close()
    return

with DAG(
    dag_id="hdhs_conn_postgres",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_test
    )

    oracle_conn_test_task