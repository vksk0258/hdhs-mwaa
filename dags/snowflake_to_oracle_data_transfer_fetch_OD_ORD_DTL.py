import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from snowflake.connector import connect as snowflake_connect
from snowflake.connector.errors import DatabaseError, ProgrammingError
import cx_Oracle
import logging
import time

def fetch_data_from_snowflake(cursor, last_max_id, batch_size=1000000):
    while True:
        logging.info(f"### ORD_NO before processing query: {last_max_id} ###")
        query = f"""
        SELECT *
        FROM HDHS_OD.OD_ORD_DTL
        WHERE ORD_NO > '{last_max_id}'
        ORDER BY ORD_NO
        LIMIT {batch_size}
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        if not rows:
            break
        yield rows
        last_max_id = rows[-1][0]  # ID가 첫 번째 열에 있다고 가정
        logging.info(f"### ORD_NO after processing query: {last_max_id} ###")

def extract_data_from_snowflake(batch_size=1000000):
    last_max_id = Variable.get("snowflake_last_max_id_od_ord_dtl", default_var='19991231000000')
    logging.info(f"### Starting last_max_id: {last_max_id} ###")

    logging.info("### Connecting to Snowflake ... ###")
    conn = snowflake_connect(
        user='hdhsdw',
        password='Ghatyvld1!',
        account='jsiterj-aa97982',
        warehouse='COMPUTE_WH',
        database='HYUNDAI_DB',
        schema='HDHS_OD',
        connect_timeout=600,
        network_timeout=180,
        login_timeout=120,
        requests_timeout=240,
        client__prefetch_threads=4,
        socket_timeout=300
    )

    try:
        cursor = conn.cursor()
        for rows in fetch_data_from_snowflake(cursor, last_max_id, batch_size):
            logging.info(f"Fetched {len(rows)} rows from Snowflake")
            insert_data_to_oracle(rows)
            last_max_id = rows[-1][0]
            Variable.set("snowflake_last_max_id_od_ord_dtl", str(last_max_id))
            logging.info(f"Change last_max_id to: {last_max_id}")
            logging.info(f"###### Success one cycle !!! ######")
    except (DatabaseError, ProgrammingError) as e:
        logging.error(f"Error: {e}")
    finally:
        cursor.close()
        conn.close()
        logging.info("### Data extraction from Snowflake completed ###")

def get_oracle_table_columns(cursor, owner, table_name):
    logging.info("### Getting column names from Oracle table ###")
    cursor.execute(f"""
        SELECT COLUMN_NAME
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = '{owner.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        ORDER BY COLUMN_ID
    """)
    columns = [row[0] for row in cursor.fetchall()]
    return columns

def insert_data_to_oracle(rows):
    logging.info("### Connecting to Oracle ... ###")
    dsn = cx_Oracle.makedsn('10.203.40.168', '1521', service_name='exadb.sbnquaexacnt.hsdwvcn.oraclevcn.com')
    conn = cx_Oracle.connect(user='HMART_DEV', password='rhdxhddnsdud1!', dsn=dsn)
    cursor = conn.cursor()

    owner = 'HMART_DEV'
    table_name = 'OD_ORD_DTL_SF_TEST'
    columns = get_oracle_table_columns(cursor, owner, table_name)
    column_count = len(columns)

    placeholders = ', '.join([f':{i+1}' for i in range(column_count)])
    insert_query = f"""
        INSERT INTO {owner}.{table_name} ({', '.join(columns)})
        VALUES ({placeholders})
    """

    logging.info("### Inserting data into Oracle ... ###")
    cursor.executemany(insert_query, rows)
    conn.commit()

    cursor.close()
    conn.close()
    logging.info("### Data inserted into Oracle !!! ###")

def extract_data_with_retries(batch_size=1000000):
    max_retries = 5
    initial_delay = 10  # 초
    attempt = 0
    
    while attempt < max_retries:
        try:
            extract_data_from_snowflake(batch_size)
            return
        except Exception as e:
            logging.error(f"{attempt + 1}번째 시도 실패: {e}")
            attempt += 1
            time.sleep(initial_delay * (2 ** attempt))  # 지수 백오프
    raise Exception("최대 재시도 횟수를 초과했습니다")

def transfer_data(batch_size=1000000):
    start_time = datetime.now()
    logging.info("### Starting data transfer SnowFlake to Oracle ... ###")

    extract_data_with_retries(batch_size)
    
    end_time = datetime.now()
    logging.info(f"### Data transfer completed in {end_time - start_time} ###")

    return "Data transfer completed"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 20),
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': True,
}
dag = DAG(
    dag_id='snowflake_to_oracle_data_transfer_fetch_OD_ORD_DTL',
    default_args=default_args,
    description='Transfer data from Snowflake to Oracle _fetch_OD_ORD_DTL',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
)

transfer_task = PythonOperator(
    task_id='transfer_data_from_snowflake_to_oracle_fetch_OD_ORD_DTL',
    python_callable=transfer_data,
    op_args=[1000000],  # 배치 사이즈를 매개변수로 전달
    dag=dag,
)

transfer_task


