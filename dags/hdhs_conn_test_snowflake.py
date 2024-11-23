from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

def isrt_data():
    query = "INSERT INTO PILOTDB.AIRFLOW.HDHS_TEST (NUM, NAME) VALUES (1, 'Test Name');"
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snow_itsmart')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    cursor.close()
    conn.close()

with DAG(
    dag_id='hdhs_conn_tset_snowflake',
    schedule_interval=None
) as dag:

    hdhs_task = PythonOperator(
        task_id='hdhs_task',
        python_callable=isrt_data,
    )
    hdhs_task