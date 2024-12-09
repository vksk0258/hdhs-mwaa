from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

def oracle_conn_test():
    jdbc_hook = JdbcHook(conn_name_attr='conn_informix_locus1',conn_type="jdbc")
    connection = jdbc_hook.get_conn()
    cursor = connection.cursor()
    print(cursor)
    cursor.close()
    connection.close()
    return

with DAG(
    dag_id="hdhs_conn_informix",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_test
    )

    oracle_conn_test_task