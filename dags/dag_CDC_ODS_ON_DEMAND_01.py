from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from operator.common import reverse_task_variables as var
import numpy as np
from common.notify_error_functions import notify_api_on_error
import pandas as pd
import pendulum
import boto3
import json

# S3 parameters
s3 = boto3.client('s3')
bucket_name = var.bucket_name
key = var.daily_key
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

daily_p_start = params.get("$$P_START")
daily_p_end = params.get("$$P_END")

key = var.on_demand_key
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

on_demand_p_start = params.get("$$P_START")
on_demand_p_end = params.get("$$P_END")

KST = pendulum.timezone("Asia/Seoul")

condition_query = f"""APLY_DT >= TO_CHAR(DATEADD(DAY, 1,TO_DATE('{daily_p_start}','YYYYMMDD')),'YYYYMMDD') 
                    AND APLY_DT <= TO_CHAR(DATEADD(DAY, 1,TO_DATE('{daily_p_end}','YYYYMMDD')),'YYYYMMDD')"""

# Column definitions
RAR_REAL_SWRT_ETC_DTL_COLUMNS = [
    "BROD_MDA_GBCD", "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD",
    "ITEM_M_CSF_CD", "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD",
    "BROD_DT", "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM",
    "ITEM_S_CSF_NM", "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM",
    "DEPT_ORGN_NM", "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY",
    "CNCL_QTY", "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT",
    "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM",
    "ETL_DTM"
]
RAR_REAL_SWRT_ONLN_DTL_COLUMNS = [
    "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD", "ITEM_M_CSF_CD",
    "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD", "BROD_DT",
    "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM",
    "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM",
    "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY",
    "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT", "RGST_ID",
    "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM",
    "ETL_DTM"
]
RAR_REAL_SWRT_ONLN_ETC_DTL_COLUMNS = [
    "BROD_MDA_GBCD", "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD",
    "ITEM_L_CSF_CD", "ITEM_M_CSF_CD", "ITEM_S_CSF_CD", "ITEM_D_CSF_CD",
    "BRND_CD", "SLITM_CD", "BROD_DT", "BROD_STRT_DTM", "BROD_TITL",
    "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM", "ITEM_D_CSF_NM",
    "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM", "PART_NM", "SLITM_NM",
    "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY", "RTP_QTY", "RTP_CNCL_QTY",
    "EXCH_CNT", "REAL_SWRT", "RGST_ID", "RGST_IP", "REG_DTM",
    "CHGP_ID", "CHGP_IP", "CHG_DTM", "ETL_DTM"
]
RAR_REAL_SWRT_DTL_COLUMNS = [
    "APLY_DT", "BFMT_NO", "SELL_MDA_GBCD", "ITEM_L_CSF_CD", "ITEM_M_CSF_CD",
    "ITEM_S_CSF_CD", "ITEM_D_CSF_CD", "BRND_CD", "SLITM_CD", "BROD_DT",
    "BROD_STRT_DTM", "BROD_TITL", "ITEM_L_CSF_NM", "ITEM_M_CSF_NM", "ITEM_S_CSF_NM",
    "ITEM_D_CSF_NM", "BRND_NM", "MD_CD", "MD_NM", "DEPT_ORGN_NM",
    "PART_NM", "SLITM_NM", "TOT_ORD_QTY", "RORD_QTY", "CNCL_QTY",
    "RTP_QTY", "RTP_CNCL_QTY", "EXCH_CNT", "REAL_SWRT", "RGST_ID",
    "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM", "ETL_DTM"
]

etl_conn_id = 'conn_snowflake_etl_temp'
load_conn_id = 'conn_snow_load'

RAR_REAL_SWRT_DTL_etl_table = "DW_RM.RAR_REAL_SWRT_DTL"
RAR_REAL_SWRT_DTL_load_table = "MWAA.RAR_REAL_SWRT_DTL"
RAR_REAL_SWRT_ETC_DTL_etl_table = "DW_RM.RAR_REAL_SWRT_ETC_DTL"
RAR_REAL_SWRT_ETC_DTL_load_table = "MWAA.RAR_REAL_SWRT_ETC_DTL"
RAR_REAL_SWRT_ONLN_DTL_etl_table = "DW_RM.RAR_REAL_SWRT_ONLN_DTL"
RAR_REAL_SWRT_ONLN_DTL_load_table = "MWAA.RAR_REAL_SWRT_ONLN_DTL"
RAR_REAL_SWRT_ONLN_ETC_DTL_etl_table = "DW_RM.RAR_REAL_SWRT_ONLN_ETC_DTL"
RAR_REAL_SWRT_ONLN_ETC_DTL_load_table = "MWAA.RAR_REAL_SWRT_ONLN_ETC_DTL"

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

    chunk_index = 1

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

        with load_connection.cursor() as load_cursor:

            create_temp_table_query = f"""
                        CREATE TEMPORARY TABLE {load_schema}.{temp_table} AS
                        SELECT * FROM {load_table} WHERE 1=0;
                        """  # 빈 임시 테이블 생성
            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

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


    load_connection.close()
    print("커넥션 종료")


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_ON_DEMAND_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:

    task_RAR_REAL_SWRT_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, RAR_REAL_SWRT_DTL_etl_table, RAR_REAL_SWRT_DTL_load_table, RAR_REAL_SWRT_DTL_COLUMNS, ['APLY_DT','BFMT_NO','BRND_CD','ITEM_D_CSF_CD','ITEM_L_CSF_CD','ITEM_M_CSF_CD','ITEM_S_CSF_CD','SELL_MDA_GBCD','SLITM_CD'],
                 condition_query],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, RAR_REAL_SWRT_ETC_DTL_etl_table, RAR_REAL_SWRT_ETC_DTL_load_table, RAR_REAL_SWRT_ETC_DTL_COLUMNS, ['APLY_DT','BFMT_NO','BRND_CD','BROD_MDA_GBCD','ITEM_D_CSF_CD','ITEM_L_CSF_CD','ITEM_M_CSF_CD','ITEM_S_CSF_CD','SELL_MDA_GBCD','SLITM_CD'],
                 condition_query],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, RAR_REAL_SWRT_ONLN_DTL_etl_table, RAR_REAL_SWRT_ONLN_DTL_load_table, RAR_REAL_SWRT_ONLN_DTL_COLUMNS, ['APLY_DT','BFMT_NO','BRND_CD','ITEM_D_CSF_CD','ITEM_L_CSF_CD','ITEM_M_CSF_CD','ITEM_S_CSF_CD','SELL_MDA_GBCD','SLITM_CD'],
                 condition_query],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, RAR_REAL_SWRT_ONLN_ETC_DTL_etl_table, RAR_REAL_SWRT_ONLN_ETC_DTL_load_table, RAR_REAL_SWRT_ONLN_ETC_DTL_COLUMNS, ['APLY_DT','BFMT_NO','BRND_CD','BROD_MDA_GBCD','ITEM_D_CSF_CD','ITEM_L_CSF_CD','ITEM_M_CSF_CD','ITEM_S_CSF_CD','SELL_MDA_GBCD','SLITM_CD'],
                 condition_query],
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_RAR_REAL_SWRT_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS






