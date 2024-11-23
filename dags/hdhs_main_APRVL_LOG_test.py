from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import pendulum
import os
import oracledb


client_path = Variable.get("client_path")
def oracle_conn_main_test():
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main',thick_mode=True,thick_mode_lib_dir=client_path)
    sql = "SELECT * FROM HDHS_OD.OD_CRD_APRVL_LOG_CRYPT"
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    print(cursor)
    cursor.execute(sql)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    print("Number of rows:", len(df))
    print("Columns:", df.columns.tolist())
    print(df.head(5))
    cursor.close()
    connection.close()
    return

with DAG(
    dag_id="hdhs_main_APRVL_LOG_test",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test
    )

    oracle_conn_test_task