from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

# Snowflake 쿼리 (프로시저 호출)
CALL_PROCEDURE_QUERY = """
CALL DW_LOAD_DB.CONFIG.PROC_CDC_SUB(
    'DW_LOAD_DB',
    'HDHS_OD',
    'CU_ARS_LDIN_MST_CRYPT',
    '2024/12/23/CU_ARS_LDIN_MST_CRYPT_20241223_030000.csv','2024/12/23/20241223_040000.csv''',
    '20241223120000',
    '20241223150000'
)
"""

# DAG 정의
with DAG(
    dag_id='snowflake_procedure_call',
    start_date=datetime(2023, 12, 1),  # 시작 날짜
    schedule_interval=None,            # 비정기 실행
    catchup=False                      # 이전 실행 안 함
) as dag:

    # SnowflakeOperator 사용
    call_procedure = SnowflakeOperator(
        task_id='call_snowflake_procedure',
        snowflake_conn_id='my_snowflake_conn',  # Airflow에 설정한 연결 ID
        sql=CALL_PROCEDURE_QUERY,              # 실행할 SQL
    )
