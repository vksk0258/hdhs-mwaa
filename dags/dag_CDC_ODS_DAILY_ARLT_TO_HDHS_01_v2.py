from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import numpy as np
import pandas as pd
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

# p_start = '20250210'
# p_end = '20250210'

KST = pendulum.timezone("Asia/Seoul")

reverse_condition_query = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

forward_condition_query = f"""
CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') + 1
AND CHG_DTM <= TO_DATE('{p_start}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

etl_conn_id = 'conn_snowflake_etl_temp'
load_conn_id = 'conn_snow_load'
etl_table = 'DW_HSIS.HES_RNTL_ARLT_DTL'
load_table = 'MWAA.HES_RNTL_ARLT_DTL'

columns = [
    "SMR_DT", "SLITM_CD", "SELL_MDA_GBCD", "BRND_CD", "MD_CD", "DEPT_ORGN_CD", "PART_ORGN_CD",
    "RNTL_ITEM_TYPE_GBCD", "SELL_AMT", "MM_PYM_AMT", "INSM_MTHS", "ORD_QTY", "EXP_SWRT",
    "ARLT_AMT", "ETL_DTM", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"
]

def snow_to_snow_merge(etl_conn_id, load_conn_id, etl_table, load_table, columns, pk_columns, condition_query):
    import os
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    etl_schema, etl_table_name = etl_table.split('.')
    load_schema, load_table_name = load_table.split('.')


    etl_hook = SnowflakeHook(snowflake_conn_id=etl_conn_id)
    load_hook = SnowflakeHook(snowflake_conn_id=load_conn_id)
    load_connection = load_hook.get_conn()

    offset = 0
    chunk_index = 1
    batch_size=200000


    with etl_hook.get_conn() as etl_connection:

        temp_table = f"{load_table_name}{chunk_index}"
        query = f"""
                    SELECT {', '.join(columns)}
                    FROM {etl_table}
                """
        query += f"""WHERE {condition_query}
                """

        print("==================[ORACLE QEURY]==================")
        print(query)
        df = pd.read_sql(query, etl_connection)

        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        # NaN 값을 명확하게 None으로 변환
        df.replace({np.nan: None}, inplace=True)

        create_temp_table_query = f"""
            CREATE TEMPORARY TABLE {load_schema}.{temp_table} AS
            SELECT * FROM {load_table} WHERE 1=0;
            """  # 빈 임시 테이블 생성
        print("==================[create_temp_table_query]==================")
        print(create_temp_table_query)

        with load_connection.cursor() as load_cursor:
            load_cursor.execute(create_temp_table_query)

            insert_query = f"""
                INSERT INTO {load_schema}.{temp_table} ({", ".join(columns)})
                VALUES ({", ".join(["%s"] * len(columns))});
                """
            print("==================[insert_query]==================")
            print(insert_query)

            # MERGE 쿼리 생성 (배치 처리)
            values = df.where(pd.notnull(df), None).values.tolist()
            load_cursor.executemany(insert_query, values)  # batch insert 실행

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            merge_query = f"""
                MERGE INTO {load_table} AS target
                USING {load_schema}.{temp_table} AS source
                ON {merge_condition}
                WHEN MATCHED THEN
                    UPDATE SET {update_set}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values});
                """

            print("==================[merge_query]==================")
            print(merge_query)

            load_cursor.execute(merge_query)

            offset += batch_size
            chunk_index += 1


    load_connection.close()
    print("커넥션 종료")

with DAG(
    dag_id="dag_CDC_ODS_DAILY_ARLT_TO_HDHS_01_v2",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_0630_DAILY_BROAD_01","ODS","역방향"]
) as dag:
    # task_HES_RNTL_ARLT_DTL_TO_HDHS = PythonOperator(
    #     task_id="task_HES_RNTL_ARLT_DTL_TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[etl_conn_id,load_conn_id, etl_table, load_table, columns,['SELL_MDA_GBCD','SLITM_CD','SMR_DT'],reverse_condition_query]
    # )

    task_HES_RNTL_ARLT_DTL = PythonOperator(
        task_id="task_HES_RNTL_ARLT_DTL",
        python_callable=snow_to_snow_merge,
        op_args=[load_conn_id, etl_conn_id, load_table, etl_table, columns, ['SELL_MDA_GBCD', 'SLITM_CD', 'SMR_DT'],
                 forward_condition_query]
    )

    task_HES_RNTL_ARLT_DTL
