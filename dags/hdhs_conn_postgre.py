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


with DAG(
    dag_id="hdhs_conn_postgres",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_test
    )
    def insrt_postgres(ip, port, dbname, user, passwd, **kwargs):
        import psycopg2
        from contextlib import closing

        with closing(psycopg2.connect(host=ip, dbname=dbname, user=user, password=passwd, port=int(port))) as conn:
            with closing(conn.cursor()) as cursor:
                print(cursor)
    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insrt_postgres,
        op_args=['10.4.128.11', '5432', 'pg_am_prd', 'hshs_reading', 'hshop1!']
    )

    [oracle_conn_test_task,insrt_postgres]