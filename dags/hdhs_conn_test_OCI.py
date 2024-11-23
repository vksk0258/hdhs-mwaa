from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import pandas as pd
import pendulum

def oracle_conn_test():
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_H2O')
    sql = "SELECT * FROM HDHS_OD.OD_STLM_INF_CRYPT WHERE CHG_DTM BETWEEN TO_DATE('2024-11-20 13:00:00', 'YYYY-MM-DD HH24:MI:SS') AND TO_DATE('2024-11-20 14:00:00', 'YYYY-MM-DD HH24:MI:SS')"
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    print(cursor)
    cursor.execute(sql)
    data = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    print("Number of rows:", len(df))
    cursor.close()
    connection.close()
    return

with DAG(
    dag_id="hdhs_conn_test_OCI",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_test
    )

    oracle_conn_test_task