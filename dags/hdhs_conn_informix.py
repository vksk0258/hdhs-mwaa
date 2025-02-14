from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
import os
import pandas as pd
import pendulum


informix_odbc = Variable.get("informix_odbc")
informix_jdbc = Variable.get("informix_jdbc")
informix_jdbc_jc = Variable.get("informix_jdbc_jc")
query = Variable.get("query")
def jdbc_conn_test():
    jdbc_hook = JdbcHook(jdbc_conn_id='conn_informix_locus1', driver_path=informix_jdbc,driver_class=informix_jdbc_jc)
    connection = jdbc_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    print(data)
    cursor.close()
    connection.close()
    return data


with DAG(
    dag_id="hdhs_conn_informix",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    jdbc_conn_test_task = PythonOperator(
        task_id='jdbc_conn_test_task',
        python_callable=jdbc_conn_test
    )


    jdbc_conn_test_task