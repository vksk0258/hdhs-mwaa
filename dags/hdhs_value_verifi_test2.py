import os
import pandas as pd
import math
from airflow.models import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import datetime

client_path = Variable.get("client_path")
tmp_dir = "/tmp/airflow_data"  # 임시 데이터 저장 디렉토리
os.makedirs(tmp_dir, exist_ok=True)  # 디렉토리 생성

table_list = [
    "HDHS_OD.OD_BASKT_INF"
]

def oracle_value_extract(table_name):
    """Oracle 데이터를 추출하여 CSV 파일로 저장."""
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
    ora_query = f"SELECT cust_no, REG_DTM, CHG_DTM FROM {table_name}"
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(ora_query).fetchall()
    cursor.close()
    connection.close()

    # CSV 파일로 저장
    oracle_file = os.path.join(tmp_dir, f"{table_name.replace('.', '_')}_oracle.csv")
    with open(oracle_file, 'w') as file:
        file.write("cust_no,reg_dtm,chg_dtm\n")
        for row in result:
            file.write(",".join(str(item) for item in row) + "\n")
    return oracle_file


def snow_value_extract(table_name):
    """Snowflake 데이터를 추출하여 CSV 파일로 저장."""
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    snow_query = f"SELECT cust_no, REG_DTM, CHG_DTM FROM DW_LOAD_DB.{table_name}"
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(snow_query).fetchall()
    cursor.close()
    connection.close()

    # CSV 파일로 저장
    snowflake_file = os.path.join(tmp_dir, f"{table_name.replace('.', '_')}_snowflake.csv")
    with open(snowflake_file, 'w') as file:
        file.write("cust_no,reg_dtm,chg_dtm\n")
        for row in result:
            file.write(",".join(str(item) for item in row) + "\n")
    return snowflake_file


def insert_comparison_results(table_name, **kwargs):
    import csv
    from datetime import datetime

    # 파일 경로 받아오기
    oracle_file = kwargs['oracle_file']
    snowflake_file = kwargs['snowflake_file']

    # 데이터 로드
    oracle_data = []
    with open(oracle_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            oracle_data.append(row)

    snowflake_data = []
    with open(snowflake_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            snowflake_data.append(row)

    # 데이터 비교
    oracle_set = {row['cust_no'] for row in oracle_data}
    snowflake_set = {row['cust_no'] for row in snowflake_data}

    only_in_oracle = oracle_set - snowflake_set
    only_in_snowflake = snowflake_set - oracle_set

    result_data = []

    for row in oracle_data:
        if row['cust_no'] in only_in_oracle:
            result_data.append({
                'cust_no': row['cust_no'],
                'snow': 'n',
                'ora': 'y',
                'reg_dtm': row['reg_dtm'],
                'chg_dtm': row['chg_dtm']
            })

    for row in snowflake_data:
        if row['cust_no'] in only_in_snowflake:
            result_data.append({
                'cust_no': row['cust_no'],
                'snow': 'y',
                'ora': 'n',
                'reg_dtm': row['reg_dtm'],
                'chg_dtm': row['chg_dtm']
            })

    # 데이터 정리 및 길이 제한
    for row in result_data:
        row['reg_dtm'] = datetime.strptime(row['reg_dtm'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')
        row['chg_dtm'] = datetime.strptime(row['chg_dtm'], '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')

    # Snowflake Hook 연결
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()

    # INSERT 쿼리
    insert_query = """
    INSERT INTO dw_load_db.config.temp (
        cust_no, 
        snow,
        ora,
        reg_dtm,
        chg_dtm
    )
    VALUES (%s, %s, %s, %s, %s)
    """

    # 배치 처리
    batch_size = 10000
    for i in range(0, len(result_data), batch_size):
        batch = result_data[i:i + batch_size]
        batch_values = [(row['cust_no'], row['snow'], row['ora'], row['reg_dtm'], row['chg_dtm']) for row in batch]
        cursor.executemany(insert_query, batch_values)
        print(f"Inserted batch {i // batch_size + 1} with {len(batch)} rows")

    connection.commit()
    cursor.close()
    connection.close()


with DAG(
        dag_id="hdhs_value_verifi_test2",
        schedule_interval=None,
        dagrun_timeout=datetime.timedelta(minutes=300),
        tags=["현대홈쇼핑"]
) as dag:
    # Task 그룹 저장 리스트
    task_groups = []

    for table in table_list:
        # Oracle 데이터 추출 Task
        oracle_task = PythonOperator(
            task_id=f'oracle_value_extract_{table.replace('.', '_')}',
            python_callable=oracle_value_extract,
            op_args=[table]
        )

        # Snowflake 데이터 추출 Task
        snow_task = PythonOperator(
            task_id=f'snow_value_extract_{table.replace('.', '_')}',
            python_callable=snow_value_extract,
            op_args=[table]
        )

        # 비교 및 결과 삽입 Task
        comparison_task = PythonOperator(
            task_id=f'insert_comparison_results_{table.replace('.', '_')}',
            python_callable=insert_comparison_results,
            op_args=[table],
            provide_context=True,
            op_kwargs={
                'oracle_file': f"{tmp_dir}/{table.replace('.', '_')}_oracle.csv",
                'snowflake_file': f"{tmp_dir}/{table.replace('.', '_')}_snowflake.csv"
            }
        )

        # 병렬 작업 후 비교 작업 실행
        [oracle_task, snow_task] >> comparison_task
