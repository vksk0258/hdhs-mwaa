from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
import boto3
import json

md_ven_intl_setup_dtl_columns = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
item_intl_dtl_columns = ["ALML_CD", "SLITM_CD", "ALML_ITEM_CD", "ALML_CO_GBCD", "ALML_INTL_NO", "SOON_USE_PRMO_NO", "SOON_USE_PRMO_PRC", "ADD_DC_PRMO_NO", "ADD_DC_PRMO_PRC", "SELL_PRC", "ALML_SELL_GBCD", "SELL_GBCD", "ITNT_DISP_YN", "ITEM_PRC_APLY_DTM", "VEN_CD", "VEN2_CD", "OSHP_VEN_ADR_SEQ", "RTP_EXCH_VEN_ADR_SEQ", "SDLVC_VEN_SEQ", "DLVC_PAY_GBCD", "BNDL_DLVC_GBCD", "NCHG_DLV_BSIC_AMT", "DLVC_BSIC_QTY", "DLV_COST", "RTP_DLV_COST", "EXCH_DLV_COST", "SEND_DTM", "ALML_INTL_RST_GBCD", "ALML_ERR_CD", "ALML_ERR_MSG", "ORGL_ALML_ITEM_CD", "ALML_APRVL_STAT_CD", "ALML_PRC_APRVL_STAT_CD", "RJT_PTC_RSN", "RPROC_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]
intl_excp_setup_dtl_columns = ["ALML_CD", "MD_CD", "VEN_CD", "VEN2_CD", "ITEM_INTL_GBCD", "ITEM_INTL_PTC_CD", "RMRK", "INTL_YN", "CHG_YN", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"]

# S3 JSON 파라미터 로드
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = f"""{params.get("$$P_START")}000000"""
p_end = f"""{params.get("$$P_END")}235959"""

# 문자열 변환
fm_p_start = f"{p_start[:4]}-{p_start[4:6]}-{p_start[6:8]} {p_start[8:10]}:{p_start[10:12]}:{p_start[12:14]}"
fm_p_end = f"{p_end[:4]}-{p_end[4:6]}-{p_end[6:8]} {p_end[8:10]}:{p_end[10:12]}:{p_end[12:14]}"

postgres_hook = PostgresHook(postgres_conn_id='conn_postgres_hdhs_reading')
snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

def split_dataframe(df, chunk_size):
    for i in range(0, len(df), chunk_size):
        yield df.iloc[i : i + chunk_size]

with DAG(
    dag_id="dag_CDC_ODS_SUB_ALLI_01",
    schedule_interval=None,
    tags=["현대홈쇼핑"]
) as dag:
    @task(task_id='task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I')
    def task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I(**kwargs):

        with postgres_hook.get_conn() as postgres_conn:
            query ="select * from AM_ALML_MD_VEN_INTL_SETUP_DTL"
            df = pd.read_sql(query, postgres_conn)

        print(df)

        with snowflake_hook.get_conn() as snowflake_conn:
            with snowflake_conn.cursor() as cursor:
                truncate_query = f"""
                truncate DW_LOAD_DB.ODS_ALLI.AM_ALML_MD_VEN_INTL_SETUP_DTL
                """
                cursor.execute(truncate_query)
                print("Truncate 완료")

        engine = snowflake_hook.get_sqlalchemy_engine()
        batch_size = 50000
        for batch_df in split_dataframe(df, batch_size):
            batch_df.to_sql("AM_ALML_MD_VEN_INTL_SETUP_DTL", con=engine, schema="ODS_ALLI", if_exists='append', index=False)
            print("적재완료")


    @task(task_id='task_AM_ALML_INTL_EXCP_SETUP_DTL_I')
    def task_AM_ALML_INTL_EXCP_SETUP_DTL():
        with postgres_hook.get_conn() as postgres_conn:
            query =f"""
                    SELECT *
                    FROM AM_ALML_INTL_EXCP_SETUP_DTL
                    WHERE CHG_DTM BETWEEN '{fm_p_start}' AND '{fm_p_end}';
                    """
            df = pd.read_sql(query, postgres_conn)
            print(query)
            print(df)

        with snowflake_hook.get_conn() as snowflake_conn:
            with snowflake_conn.cursor() as cursor:
                truncate_query = f"""
                truncate DW_LOAD_DB.ODS_ALLI.AM_ALML_INTL_EXCP_SETUP_DTL_TEMP
                """
                cursor.execute(truncate_query)
                print("Truncate 완료")

        engine = snowflake_hook.get_sqlalchemy_engine()
        batch_size = 50000
        for batch_df in split_dataframe(df, batch_size):
            batch_df.to_sql("AM_ALML_INTL_EXCP_SETUP_DTL_TEMP", con=engine, schema="ODS_ALLI", if_exists='append',
                            index=False)
            print("적재완료")


    @task(task_id='task_AM_ALML_ITEM_INTL_DTL_I')
    def task_AM_ALML_ITEM_INTL_DTL():
        with postgres_hook.get_conn() as postgres_conn:
            query = f"""
                        SELECT *
                        FROM AM_ALML_ITEM_INTL_DTL
                        WHERE CHG_DTM BETWEEN '{fm_p_start}' AND '{fm_p_end}'
                        """
            # df = pd.read_sql(query, postgres_conn)
            print(query)
            with snowflake_hook.get_conn() as snowflake_conn:
                with snowflake_conn.cursor() as cursor:
                    truncate_query = f"""
                                        truncate DW_LOAD_DB.ODS_ALLI.AM_ALML_ITEM_INTL_DTL_TEMP
                                        """
                    cursor.execute(truncate_query)
                    print("Truncate 완료")
                    engine = snowflake_hook.get_sqlalchemy_engine()
                    for chunk in pd.read_sql(query, postgres_conn, chunksize=50000):
                        chunk.to_sql("AM_ALML_ITEM_INTL_DTL_TEMP", con=engine, schema="ODS_ALLI", if_exists='append',
                                        index=False)
                        print("적재완료")


    task_AM_ALML_MD_VEN_INTL_SETUP_DTL_I() >> task_AM_ALML_INTL_EXCP_SETUP_DTL() >> task_AM_ALML_ITEM_INTL_DTL()