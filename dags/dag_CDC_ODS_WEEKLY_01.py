from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from common.notify_error_functions import notify_api_on_error
from decimal import Decimal
from airflow.decorators import task
import boto3
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import pendulum

KST = pendulum.timezone("Asia/Seoul")

var_text = Variable.get('VAR_dag_CDC_ODS_WEEKLY_TO_HDHS')

var_dict = json.loads(var_text)

client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_WW01_MON_0630_WEEKLY_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

def reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st):
    ora_main_hook = OracleHook(oracle_conn_id='conn_oracle_H2O', thick_mode=True, thick_mode_lib_dir=client_path)

    if task_name == 'CU_CUST_MKTG_AGR_MST_TO_HDHS':
        task_name = 'CU_CUST_MKTG_MST_TO_HDHS'

    with ora_main_hook.get_conn() as ora_connection:
        select_query = f"""
        SELECT SUCCESSFUL_ROWS, ACTUAL_START
        FROM INFA.REP_SESS_LOG
        WHERE SESSION_INSTANCE_NAME LIKE '%{task_name}%'
        ORDER BY SESSION_TIMESTAMP DESC
        FETCH FIRST 1 ROW ONLY
        """
        infa_df = pd.read_sql(select_query, ora_connection)

    if infa_df.empty:
        raise ValueError(f"No data found for task_name: {task_name}")

    infa_cnt = infa_df['SUCCESSFUL_ROWS'].iloc[0]
    infa_start_time = infa_df['ACTUAL_START'].iloc[0]
    infa_start_time_str = infa_start_time.strftime('%Y-%m-%d %H:%M:%S')

    snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
    with snow_hook.get_conn() as snow_connection:
        with snow_connection.cursor() as snow_cursor:
            ist_query = f"""
            INSERT INTO DW_ETC.REVERSE_BATCH_VERIFI (
            VERIFY_DATE, 
            TYPE, 
            SCHEMA_NAME, 
            TABLE_NAME, 
            INFA_CNT, 
            MWAA_CNT, 
            INFA_MINUS_MWAA_CNT, 
            INFA_START_TIME, 
            MWAA_START_TIME)
            VALUES (
            '{mwaa_st.strftime('%Y-%m-%d %H:%M:%S')}',
            '{type}',
            '{schema_name}',
            '{table_name}',
            {infa_cnt},
            {mwaa_cnt},
            {infa_cnt - mwaa_cnt},
            '{infa_start_time_str}',
            '{mwaa_st.strftime('%Y-%m-%d %H:%M:%S')}'
            )
            """
            print(ist_query)
            snow_cursor.execute(ist_query)

def snow_to_snow_merge(snow_conn_id, ora_main_conn_id, snow_table, ora_main_table, columns, pk_columns, condition_query, **kwargs):
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
    ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
    ora_main_connection = ora_main_hook.get_conn()

    ora_schema, ora_table_name = ora_main_table.split('.')

    with snow_hook.get_conn() as snow_connection:
        with ora_main_connection.cursor() as ora_main_cursor:

            truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            print("==================[TRUNCATE QEURY]==================")
            print(truncate_query)
            ora_main_cursor.execute(truncate_query)

            query = f"""
                        SELECT {', '.join(columns)}
                        FROM {snow_table}
                     """
            query += f"""{condition_query}
                    """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].astype(float).where(df[col].notnull(), None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df.select_dtypes(include=['datetime', 'datetimetz']).columns

            # Insert Query 수정
            insert_query = f"""
                INSERT INTO {ora_main_table} ({", ".join(columns)})
                VALUES ({", ".join([f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}" for i, col in enumerate(columns)])})
            """

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df = df.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col] for
                col in columns], axis=1).tolist()

            print("==================[INSERT QEURY]==================")
            print(insert_query)

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df), batch_size):
                if len(df) == 0:
                    print("조회 0건")
                    break
                batch = df[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i+10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

    ora_main_connection.close()
    print("커넥션 종료")

    task_name = kwargs['ti'].task_id[5:]
    type = "weekly"
    schema_name = ora_schema
    table_name = ora_table_name
    mwaa_cnt = len(df)
    mwaa_st = pendulum.now(KST)
    reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)


snow_conn_id = 'conn_snowflake_etl'
ora_main_conn_id = 'conn_oracle_main'

CONDITION_QUERY = f"""WHERE CHG_DTM BETWEEN TO_TIMESTAMP('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND TO_TIMESTAMP('{p_end}' || '235959', 'YYYYMMDDHH24MISS')
"""


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_WEEKLY_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:
    @task(task_id='task_HES_EXP_SWRT_DTL_TO_HDHS', provide_context=True,
          trigger_rule="all_done",
          on_failure_callback=notify_api_on_error
)
    def task_HES_EXP_SWRT_DTL_TO_HDHS(**kwargs):

        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:
            query = f"""
                                SELECT *
                                FROM {snow_table}
                             """
            query += f"""{CONDITION_QUERY}
                            """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df_tf = pd.DataFrame(index=range(len(columns)))

            df_tf['APLY_DT'] = df['APLY_DT']
            df_tf['SELL_MDA_GBCD'] = df['SELL_MDA_GBCD']
            df_tf['ITEM_L_CSF_CD'] = df['ITEM_L_CSF_CD']
            df_tf['ITEM_M_CSF_CD'] = df['ITEM_M_CSF_CD']
            df_tf['ITEM_S_CSF_CD'] = df['ITEM_S_CSF_CD']
            df_tf['ITEM_D_CSF_CD'] = df['ITEM_S_CSF_CD']
            df_tf['BRND_CD'] = df['BRND_CD']
            df_tf['ITEM_L_CSF_NM'] = df['ITEM_L_CSF_NM']
            df_tf['ITEM_M_CSF_NM'] = df['ITEM_M_CSF_NM']
            df_tf['ITEM_S_CSF_NM'] = df['ITEM_S_CSF_NM']
            df_tf['ITEM_D_CSF_NM'] = df['ITEM_D_CSF_NM']
            df_tf['BRND_NM'] = df['BRND_NM']
            df_tf['TOT_ORD_QTY'] = df['TOT_ORD_QTY']
            df_tf['RORD_QTY'] = df['RORD_QTY']
            df_tf['CNCL_QTY'] = df['ORD_CNCL_QTY']#컬럼명 변경
            df_tf['RTP_QTY'] = df['RTP_QTY']
            df_tf['RTP_CNCL_QTY'] = df['RTP_CNCL_QTY']
            df_tf['EXCH_CNT'] = df['EXCH_CNT']
            df_tf['SWRT'] = df['SWRT']
            df_tf['CNCL_RATE'] = df['CNCL_RATE']
            df_tf['RTP_RATE'] = df['RTP_RATE']
            df_tf['EXCH_RATE'] = df['EXCH_RATE']
            df_tf['RGST_ID'] = df['RGST_ID']
            df_tf['RGST_IP'] = df['RGST_IP']
            df_tf['REG_DTM'] = df['REG_DTM']
            df_tf['CHGP_ID'] = df['CHGP_ID']
            df_tf['CHGP_IP'] = df['CHGP_IP']
            df_tf['CHG_DTM'] = df['CHG_DTM']

        with ora_main_connection.cursor() as ora_main_cursor:

            ora_schema, ora_table_name = ora_main_table.split('.')

            truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            print("==================[TRUNCATE QEURY]==================")
            print(truncate_query)
            ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].astype(float).where(df_tf[col].notnull(), None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

            # Insert Query 수정
            insert_query = f"""
                                    INSERT INTO {ora_main_table} ({", ".join(columns)})
                                    VALUES ({", ".join([f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}" for i, col in enumerate(columns)])})
                                """

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df_tf = df_tf.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col] for
                col in columns], axis=1).tolist()

            print("==================[INSERT QEURY]==================")
            print(insert_query)

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                if len(df) == 0:
                    print("조회 0건")
                    break
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")

        task_name = kwargs['ti'].task_id[5:]
        type = "weekly"
        schema_name = ora_schema
        table_name = ora_table_name
        mwaa_cnt = len(df)
        mwaa_st = pendulum.now(KST)
        reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)


    @task(task_id='task_HES_EXP_SWRT_ETC_DTL_TO_HDHS', provide_context=True,
          trigger_rule="all_done",
          on_failure_callback=notify_api_on_error)
    def task_HES_EXP_SWRT_ETC_DTL_TO_HDHS(**kwargs):

        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:
            query = f"""
                        SELECT *
                        FROM {snow_table}
                     """
            query += f"""{CONDITION_QUERY}
                                """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df_tf = pd.DataFrame(index=range(len(columns)))

            df_tf['BROD_MDA_GBCD'] = df['BROD_MDA_GBCD']
            df_tf['APLY_DT'] = df['APLY_DT']
            df_tf['SELL_MDA_GBCD'] = df['SELL_MDA_GBCD']
            df_tf['ITEM_L_CSF_CD'] = df['ITEM_L_CSF_CD']
            df_tf['ITEM_M_CSF_CD'] = df['ITEM_M_CSF_CD']
            df_tf['ITEM_S_CSF_CD'] = df['ITEM_S_CSF_CD']
            df_tf['ITEM_D_CSF_CD'] = df['ITEM_S_CSF_CD']
            df_tf['BRND_CD'] = df['BRND_CD']
            df_tf['ITEM_L_CSF_NM'] = df['ITEM_L_CSF_NM']
            df_tf['ITEM_M_CSF_NM'] = df['ITEM_M_CSF_NM']
            df_tf['ITEM_S_CSF_NM'] = df['ITEM_S_CSF_NM']
            df_tf['ITEM_D_CSF_NM'] = df['ITEM_D_CSF_NM']
            df_tf['BRND_NM'] = df['BRND_NM']
            df_tf['TOT_ORD_QTY'] = df['TOT_ORD_QTY']
            df_tf['RORD_QTY'] = df['RORD_QTY']
            df_tf['CNCL_QTY'] = df['ORD_CNCL_QTY'] #컬럼명 변경
            df_tf['RTP_QTY'] = df['RTP_QTY']
            df_tf['RTP_CNCL_QTY'] = df['RTP_CNCL_QTY']
            df_tf['EXCH_CNT'] = df['EXCH_CNT']
            df_tf['SWRT'] = df['SWRT']
            df_tf['RGST_ID'] = df['RGST_ID']
            df_tf['RGST_IP'] = df['RGST_IP']
            df_tf['REG_DTM'] = df['REG_DTM']
            df_tf['CHGP_ID'] = df['CHGP_ID']
            df_tf['CHGP_IP'] = df['CHGP_IP']
            df_tf['CHG_DTM'] = df['CHG_DTM']
            df_tf['CNCL_RATE'] = df['CNCL_RATE']
            df_tf['RTP_RATE'] = df['RTP_RATE']
            df_tf['EXCH_RATE'] = df['EXCH_RATE']

        with ora_main_connection.cursor() as ora_main_cursor:

            ora_schema, ora_table_name = ora_main_table.split('.')

            truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            print("==================[TRUNCATE QEURY]==================")
            print(truncate_query)
            ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].astype(float).where(df_tf[col].notnull(), None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

            # Insert Query 수정
            insert_query = f"""
                                        INSERT INTO {ora_main_table} ({", ".join(columns)})
                                        VALUES ({", ".join([f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}" for i, col in enumerate(columns)])})
                                    """

            print(insert_query)

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df_tf = df_tf.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col] for
                col in columns], axis=1).tolist()

            print("==================[INSERT QEURY]==================")
            print(insert_query)

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                if len(df) == 0:
                    print("조회 0건")
                    break
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")

        task_name = kwargs['ti'].task_id[5:]
        type = "weekly"
        schema_name = ora_schema
        table_name = ora_table_name
        mwaa_cnt = len(df)
        mwaa_st = pendulum.now(KST)
        reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)


    task_HES_EXP_SWRT_MLB_DTL_TO_HDHS= PythonOperator(
        task_id="task_HES_EXP_SWRT_MLB_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    @task(task_id='task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS', provide_context=True,
          trigger_rule="all_done",
          on_failure_callback=notify_api_on_error)
    def task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS(**kwargs):

        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:
            query = f"""
                        SELECT *
                        FROM {snow_table}
                     """
            query += f"""{CONDITION_QUERY}
                                """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1).to_string())

            df_tf = pd.DataFrame(index=range(len(columns)))

            df_tf['APLY_DT'] = df['APLY_DT']
            df_tf['SELL_MDA_GBCD'] = df['SELL_MDA_GBCD']
            df_tf['ITEM_L_CSF_CD'] = df['ITEM_L_CSF_CD']
            df_tf['ITEM_M_CSF_CD'] = df['ITEM_M_CSF_CD']
            df_tf['ITEM_S_CSF_CD'] = df['ITEM_S_CSF_CD']
            df_tf['ITEM_D_CSF_CD'] = df['ITEM_D_CSF_CD']
            df_tf['BRND_CD'] = df['BRND_CD']
            df_tf['ITEM_L_CSF_NM'] = df['ITEM_L_CSF_NM']
            df_tf['ITEM_M_CSF_NM'] = df['ITEM_M_CSF_NM']
            df_tf['ITEM_S_CSF_NM'] = df['ITEM_S_CSF_NM']
            df_tf['ITEM_D_CSF_NM'] = df['ITEM_D_CSF_NM']
            df_tf['BRND_NM'] = df['BRND_NM']
            df_tf['TOT_ORD_QTY'] = df['TOT_ORD_QTY'].apply(lambda x: round(x, 15))
            df_tf['RORD_QTY'] = df['RORD_QTY'].apply(lambda x: round(x, 15))
            df_tf['CNCL_QTY'] = df['CNCL_QTY'].apply(lambda x: round(x, 15))
            df_tf['RTP_QTY'] = df['RTP_QTY'].apply(lambda x: round(x, 15))
            df_tf['RTP_CNCL_QTY'] = df['RTP_CNCL_QTY'].apply(lambda x: round(x, 15))
            df_tf['EXCH_CNT'] = df['EXCH_CNT'].apply(lambda x: round(x, 5))
            df_tf['SWRT'] = df['SWRT'].apply(lambda x: round(x, 5))
            df_tf['CNCL_RATE'] = df['CNCL_RATE'].apply(lambda x: round(x, 5))
            df_tf['RTP_RATE'] = df['RTP_RATE'].apply(lambda x: round(x, 5))
            df_tf['EXCH_RATE'] = df['EXCH_RATE'].apply(lambda x: round(x, 5))
            df_tf['RGST_ID'] = df['RGST_ID']
            df_tf['RGST_IP'] = df['RGST_IP']
            df_tf['REG_DTM'] = df['REG_DTM']
            df_tf['CHGP_ID'] = df['CHGP_ID']
            df_tf['CHGP_IP'] = df['CHGP_IP']
            df_tf['CHG_DTM'] = df['CHG_DTM']

            print(df_tf.head(1).to_string())

        with ora_main_connection.cursor() as ora_main_cursor:

            ora_schema, ora_table_name = ora_main_table.split('.')\

            truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            print("==================[TRUNCATE QEURY]==================")
            print(truncate_query)
            ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].apply(lambda x: None if pd.isna(x) or x is np.nan else float(x))

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

            # Insert Query 수정
            insert_query = f"""
                                        INSERT INTO {ora_main_table} ({", ".join(columns)})
                                        VALUES ({", ".join([f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}" for i, col in enumerate(columns)])})
                                    """

            print(insert_query)

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df_tf = df_tf.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col] for
                col in columns], axis=1).tolist()

            print("==================[INSERT QEURY]==================")
            print(insert_query)

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                if len(df) == 0:
                    print("조회 0건")
                    break
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")

        task_name = kwargs['ti'].task_id[5:]
        type = "weekly"
        schema_name = ora_schema
        table_name = ora_table_name
        mwaa_cnt = len(df)
        mwaa_st = pendulum.now(KST)
        reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)


    task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS= PythonOperator(
        task_id="task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS = PythonOperator(
        task_id="task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_HES_EXP_SWRT_DTL_TO_HDHS = task_HES_EXP_SWRT_DTL_TO_HDHS()
    task_HES_EXP_SWRT_ETC_DTL_TO_HDHS = task_HES_EXP_SWRT_ETC_DTL_TO_HDHS()
    task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS = task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS()

    task_HES_EXP_SWRT_DTL_TO_HDHS >> task_HES_EXP_SWRT_ETC_DTL_TO_HDHS >> task_HES_EXP_SWRT_MLB_DTL_TO_HDHS >> \
    task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS >> task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS >> task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS
