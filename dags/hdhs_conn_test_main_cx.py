from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import cx_Oracle
import pandas as pd
import pendulum
import os

client_path = Variable.get("client_path")
def oracle_conn_main_test():
    connection = cx_Oracle.connect('DMS_DW','ghaTyvlddmsdw1!2!','10.4.128.53:1522/HSHOP')
    cursor = connection.cursor()
    print(cursor)
    cursor.close()
    connection.close()
    return

with DAG(
    dag_id="hdhs_conn_test_main_cx",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test
    )

    oracle_conn_test_task