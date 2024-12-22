from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# 전역 변수 정의
client_path = Variable.get("client_path")
query = Variable.get("query")

def oracle_conn_main_test():
    # OracleHook을 사용하여 Airflow Connection으로 연결
    oracle_hook = OracleHook(oracle_conn_id='conn_oracle_main', thick_mode=True, thick_mode_lib_dir=client_path)
    sql_query = query  # 실행할 SQL 쿼리
    output_path = '/tmp/oracle_data.parquet'  # 저장할 Parquet 파일 경로

    # Oracle 데이터베이스 연결 및 데이터 가져오기
    with oracle_hook.get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute(sql_query)

        # 컬럼 이름 가져오기
        headers = [col[0] for col in cursor.description]

        # Parquet 파일을 스트림 방식으로 쓰기
        # Arrow Table과 Parquet Writer 초기화
        arrow_schema = pa.schema([(col, pa.string()) if col != 'APRVL_DTM' else (col, pa.timestamp('ns')) for col in headers])  # 데이터 타입 정의
        with pq.ParquetWriter(output_path, arrow_schema) as writer:
            while True:
                # 데이터를 한 번에 가져오는 크기 설정 (chunk size)
                rows = cursor.fetchmany(1000)  # 1000개씩 데이터를 가져옴
                if not rows:
                    break  # 더 이상 데이터가 없으면 종료

                # Pandas DataFrame으로 변환
                df = pd.DataFrame(rows, columns=headers)

                # datetime 컬럼을 처리 (예: APRVL_DTM)
                if 'APRVL_DTM' in df.columns:
                    df['APRVL_DTM'] = pd.to_datetime(df['APRVL_DTM'])  # 명시적으로 datetime 변환

                # Pandas DataFrame을 Arrow Table로 변환
                table = pa.Table.from_pandas(df)

                # Parquet 파일에 쓰기
                writer.write_table(table)

        print(f"Parquet 파일이 저장되었습니다: {output_path}")

with DAG(
    dag_id="hdhs_oracle_parquet_load",
    schedule_interval=None,
    start_date=datetime.datetime(2023, 1, 1),
    dagrun_timeout=datetime.timedelta(minutes=300),
    tags=["현대홈쇼핑"]
) as dag:
    # Variable로부터 값 가져오기
    var_value = Variable.get("sample_key")

    # Oracle에서 데이터 가져와 Parquet으로 저장하는 작업
    oracle_conn_test_task = PythonOperator(
        task_id='oracle_conn_test_task',
        python_callable=oracle_conn_main_test
    )

    # BashOperator로 bash 명령 실행
    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command=var_value
    )

    # Task 의존성 정의
    oracle_conn_test_task >> bash_var_2
