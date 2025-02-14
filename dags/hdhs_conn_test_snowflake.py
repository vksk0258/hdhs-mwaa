from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def isrt_data1():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print(conn)
    print(cursor)
    cursor.close()
    conn.close()

def isrt_data2():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print(conn)
    print(cursor)
    cursor.close()
    conn.close()

def isrt_data3():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_insu')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print(conn)
    print(cursor)
    cursor.close()
    conn.close()

def isrt_data4():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_api')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    print(conn)
    print(cursor)
    cursor.close()
    conn.close()

# def isrt_data5():
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
#     conn = snowflake_hook.get_conn()
#     cursor = conn.cursor()
#     print(conn)
#     print(cursor)
#     cursor.close()
#     conn.close()
#
# def isrt_data6():
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
#     conn = snowflake_hook.get_conn()
#     cursor = conn.cursor()
#     print(conn)
#     print(cursor)
#     cursor.close()
#     conn.close()
#
# def isrt_data7():
#     snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_etl')
#     conn = snowflake_hook.get_conn()
#     cursor = conn.cursor()
#     print(conn)
#     print(cursor)
#     cursor.close()
#     conn.close()


with DAG(
    dag_id='hdhs_conn_tset_snowflake',
    schedule_interval=None
) as dag:

    hdhs_task1 = PythonOperator(
        task_id='hdhs_task1',
        python_callable=isrt_data1,
    )

    hdhs_task2 = PythonOperator(
        task_id='hdhs_task2',
        python_callable=isrt_data2,
    )

    hdhs_task3 = PythonOperator(
        task_id='hdhs_task3',
        python_callable=isrt_data3,
    )

    hdhs_task4 = PythonOperator(
        task_id='hdhs_task4',
        python_callable=isrt_data4,
    )

    # hdhs_task5 = PythonOperator(
    #     task_id='hdhs_task',
    #     python_callable=isrt_data5,
    # )
    #
    # hdhs_task6 = PythonOperator(
    #     task_id='hdhs_task',
    #     python_callable=isrt_data6,
    # )
    #
    # hdhs_task7 = PythonOperator(
    #     task_id='hdhs_task',
    #     python_callable=isrt_data7,
    # )