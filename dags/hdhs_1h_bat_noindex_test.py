from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pandas as pd
import pprint
import time


client_path = Variable.get("client_path")
start_time_str = Variable.get("start_time")
end_time_str = Variable.get("end_time")
def oracle_conn_main_test():
    start = time.time()
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main',thick_mode=True,thick_mode_lib_dir=client_path)
    sql = f"""
    SELECT * 
    FROM HDHS_OD.OD_CRD_APRVL_LOG_CRYPT 
    WHERE CHG_DTM BETWEEN TO_DATE('{start_time_str}', 'YYYY-MM-DD HH24:MI:SS') 
                      AND TO_DATE('{end_time_str}', 'YYYY-MM-DD HH24:MI:SS')
    """
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    print(cursor)
    point1 = time.time()
    pprint.pprint(f"커넥션 붙는 시간: {point1 - start} sec")

    cursor.execute(sql)
    data = cursor.fetchall()
    point2 = time.time()
    pprint.pprint(f"쿼리 수행 시간: : {point2 - point1} sec")

    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=column_names)
    print("Number of rows:", len(df))
    cursor.close()
    connection.close()
    point3 = time.time()
    pprint.pprint(f"커넥션 끊기는데 걸리는 시간: {point3 - point2} sec")
    return

with DAG(
    dag_id="hdhs_1h_bat_noindex_test",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test
    )

    oracle_conn_test_task