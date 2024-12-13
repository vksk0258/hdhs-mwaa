from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum
import pandas as pd
import datetime
import math
import time
from airflow.models import Variable
import tempfile
import os

client_path = Variable.get("client_path")

table = "HDHS_OD.OD_BASKT_INF"

def oracle_value_extract_to_file(table_name):
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
    ora_query = "select cust_no, REG_DTM, CHG_DTM from " + table_name
    connection = oracle_hook.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(ora_query).fetchall()
    cursor.close()
    connection.close()

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix="_oracle.csv")
    pd.DataFrame(result, columns=['cust_no', 'REG_DTM', 'CHG_DTM']).to_csv(temp_file.name, index=False)
    return temp_file.name

def snow_value_extract_to_file(table_name):
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    snow_query = "select cust_no, REG_DTM, CHG_DTM from DW_LOAD_DB."+table_name+"_old"
    connection = snowflake_hook.get_conn()
    cursor = connection.cursor()
    result = cursor.execute(snow_query).fetchall()
    cursor.close()
    connection.close()

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix="_snowflake.csv")
    pd.DataFrame(result, columns=['cust_no', 'REG_DTM', 'CHG_DTM']).to_csv(temp_file.name, index=False)
    return temp_file.name

def insert_comparison_results(table_name, **kwargs):
    # 파일 경로 가져오기
    oracle_file = kwargs['ti'].xcom_pull(task_ids=f'oracle_value_extract', key='return_value')
    snowflake_file = kwargs['ti'].xcom_pull(task_ids=f'snow_value_extract', key='return_value')

    # 파일에서 데이터 읽기
    oracle_data = pd.read_csv(oracle_file)
    snowflake_data = pd.read_csv(snowflake_file)

    # 데이터 포맷 정리 (리스트 형태로 변환)
    snowflake_dict = snowflake_data.set_index('cust_no').T.to_dict('list')
    oracle_dict = oracle_data.set_index('cust_no').T.to_dict('list')

    snowflake_set = set(snowflake_dict.keys())
    oracle_set = set(oracle_dict.keys())

    # 차집합 계산
    only_in_snowflake = snowflake_set - oracle_set
    only_in_oracle = oracle_set - snowflake_set

    # 데이터프레임 생성
    snowflake_df = pd.DataFrame({
        'cust_no': list(only_in_snowflake),
        'snow': ['y'] * len(only_in_snowflake),
        'ora': ['n'] * len(only_in_snowflake),
        'reg_dtm': [snowflake_dict[cust_no][0] for cust_no in only_in_snowflake],
        'chg_dtm': [snowflake_dict[cust_no][1] for cust_no in only_in_snowflake]
    })

    oracle_df = pd.DataFrame({
        'cust_no': list(only_in_oracle),
        'snow': ['n'] * len(only_in_oracle),
        'ora': ['y'] * len(only_in_oracle),
        'reg_dtm': [oracle_dict[cust_no][0] for cust_no in only_in_oracle],
        'chg_dtm': [oracle_dict[cust_no][1] for cust_no in only_in_oracle]
    })

    # 두 데이터프레임 병합
    result_df = pd.concat([snowflake_df, oracle_df], ignore_index=True)

    # Snowflake Hook 연결
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

    def insert_batch(batch_data):
        connection = None
        try:
            connection = snowflake_hook.get_conn()
            cursor = connection.cursor()

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

            cursor.executemany(insert_query, batch_data)
            connection.commit()
            print(f"Inserted batch with {len(batch_data)} rows")
        except Exception as e:
            print(f"Error during batch insert: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                cursor.close()
                connection.close()

    # 배치 크기 설정
    batch_size = 10000
    num_batches = math.ceil(len(result_df) / batch_size)

    # 데이터프레임을 리스트로 변환 후 배치 삽입
    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = start_idx + batch_size
        batch_data = result_df.iloc[start_idx:end_idx].values.tolist()

        retry_attempts = 3
        for attempt in range(retry_attempts):
            try:
                insert_batch(batch_data)
                break
            except Exception as e:
                print(f"Retrying batch insert (attempt {attempt + 1}/{retry_attempts})...")
                time.sleep(5)
                if attempt == retry_attempts - 1:
                    raise

    # 임시 파일 삭제
    os.remove(oracle_file)
    os.remove(snowflake_file)

with DAG(
        dag_id="hdhs_value_verifi_test",
        start_date=pendulum.datetime(2024, 12, 10, tz="Asia/Seoul"),
        schedule_interval=None,
        dagrun_timeout=datetime.timedelta(minutes=300),
        tags=["현대홈쇼핑"]
) as dag:
    # Oracle 데이터 추출 Task
    oracle_task = PythonOperator(
        task_id=f'oracle_value_extract',
        python_callable=oracle_value_extract_to_file,
        op_args=[table],
        trigger_rule='none_skipped'
    )

    # Snowflake 데이터 추출 Task
    snow_task = PythonOperator(
        task_id=f'snow_value_extract',
        python_callable=snow_value_extract_to_file,
        op_args=[table],
        trigger_rule='none_skipped'
    )

    # 비교 및 결과 삽입 Task
    comparison_task = PythonOperator(
        task_id=f'insert_comparison_results',
        python_callable=insert_comparison_results,
        op_args=[table],
        trigger_rule='none_skipped'
    )

    # 병렬 작업 후 비교 작업 실행
    [oracle_task, snow_task] >> comparison_task
