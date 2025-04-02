from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from common.notify_error_functions import notify_api_on_error
from common import reverse_task_variables as var
from decimal import Decimal
import pandas as pd
from airflow.models import Variable  # Airflow 변수 관리
import pendulum
import boto3
import json

KST = pendulum.timezone("Asia/Seoul")
var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')
var_dict = json.loads(var_text)
client_path = Variable.get("client_path")

snow_conn_id = 'conn_snowflake_insu'
ora_main_conn_id = 'conn_oracle_main'

def snow_to_snow_merge2(snow_conn_id, ora_main_conn_id, snow_table, ora_main_table, columns, pk_columns, condition_query,**kwargs):
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
    ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
    ora_main_connection = ora_main_hook.get_conn()

    with snow_hook.get_conn() as snow_connection:
        with ora_main_connection.cursor() as ora_main_cursor:

            query = f"""
                        SELECT {', '.join(columns)}
                        FROM {snow_table}
                     """ + condition_query

            print("==================[ORACLE QUERY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(str(df.head(1)))

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].astype(float).where(df[col].notnull(), None)

            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df.select_dtypes(include=['datetime', 'datetimetz']).columns

            # MERGE Query 생성
            set_clause = ", ".join([
                f"{col} = :{i + 1}"
                for i, col in enumerate(columns) if col not in pk_columns
            ])

            insert_values = ", ".join([
                f":{i + 1}"
                for i, col in enumerate(columns)
            ])

            on_clause = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

            merge_query = f"""
                MERGE INTO {ora_main_table} target
                USING (SELECT {', '.join([':' + str(i + 1) + ' AS ' + col for i, col in enumerate(columns)])} FROM DUAL) source
                ON ({on_clause})
                WHEN MATCHED THEN
                    UPDATE SET {set_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(columns)})
                    VALUES ({insert_values})
            """

            # 올바른 처리 - datetime 그대로 넘김
            df = df.apply(lambda row: [
                row[col].to_pydatetime() if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            print("==================[MERRGE QUERY]==================")
            print(merge_query)

            # BATCH MERGE 수행
            batch_size = 10000
            for i in range(0, len(df), batch_size):
                batch = df[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()


                print(f"총{len(df)}건 중 {i+10000}건 커밋 완료")

    ora_main_connection.close()
    print("커넥션 종료")

task_CU_CUST_MKTG_MST_TO_HDHS_QEURY = f"""WHERE MKTG_RFS_TRGT_YN = 'Y' 
AND TO_DATE(NOTC_PRRG_DT, 'YYYYMMDD') 
BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD') + 1
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD') + 1
"""

task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS_QEURY = f"""WHERE TO_DATE(NOTC_PRRG_DT, 'YYYYMMDD') 
BETWEEN DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')) 
AND DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD')) 
AND ETL_DTM BETWEEN TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 0, 0, 0) 
AND TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 23, 59, 59)
"""

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_1200_CP_SMR_MST_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")


# Define the DAG
with DAG(
    dag_id="dag_CDC_MART_MKTG_AGR_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0010_MKTG_AGR_TERM","MART프로시져"]
) as dag:

    task_SP_BMK_CUST_MKTG_AGR_MST_01 = PythonOperator(
        task_id="task_SP_BMK_CUST_MKTG_AGR_MST_01",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_AGR_MST", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01 = PythonOperator(
        task_id="task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_AGR_EMAIL_DTL", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_CU_CUST_MKTG_AGR_MST_TO_HDHS = PythonOperator(
        task_id="task_CU_CUST_MKTG_AGR_MST_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=['conn_snowflake_insu',
                 ora_main_conn_id,
                 var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['COLUMNS'],
                 var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['PK_COLUMNS'],
                 task_CU_CUST_MKTG_MST_TO_HDHS_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS = PythonOperator(
        task_id="task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=['conn_snowflake_insu',
                 ora_main_conn_id,
                 var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['PK_COLUMNS'],
                 task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    [task_SP_BMK_CUST_MKTG_AGR_MST_01, task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01]

    task_SP_BMK_CUST_MKTG_AGR_MST_01 >> task_CU_CUST_MKTG_AGR_MST_TO_HDHS
    task_SP_BMK_CUST_MKTG_AGR_EMAIL_DTL_01 >> task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS

