from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import os
import pandas as pd
import pendulum


informix_odbc = Variable.get("informix_odbc")
informix_jdbc = Variable.get("informix_jdbc")
query = Variable.get("query")
def jdbc_conn_test():
    jdbc_hook = JdbcHook(jdbc_conn_id='conn_informix_locus1', driver_path=informix_jdbc,driver_class='com.informix.jdbc.IfxDriver')
    connection = jdbc_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    print(data)
    cursor.close()
    connection.close()
    return data

def odbc_conn_test():
    odbc_hook = OdbcHook(odbc_conn_id='conn_informix_locus1_odbc',driver=informix_odbc)
    connection = odbc_hook.get_conn()
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
    jdbc_conn_test_task = PythonOperator(
        task_id='jdbc_conn_test_task',
        python_callable=jdbc_conn_test
    )

    odbc_conn_test_task = PythonOperator(
        task_id='odbc_conn_test_task',
        python_callable=odbc_conn_test
    )

    [jdbc_conn_test_task,odbc_conn_test_task]