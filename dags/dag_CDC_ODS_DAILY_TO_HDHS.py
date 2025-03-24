from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from common.notify_error_functions import notify_api_on_error
from common import reverse_task_variables as var
from airflow.decorators import task
from decimal import Decimal
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import pendulum

KST = pendulum.timezone("Asia/Seoul")

var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')

var_dict = json.loads(var_text)

client_path = Variable.get("client_path")

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
            print(str(df.head(1)))

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].astype(float).where(df[col].notnull(), None)

            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

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
                batch = df[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i+10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

    ora_main_connection.close()
    print("커넥션 종료")


snow_conn_id = 'conn_snowflake_etl'
ora_main_conn_id = 'conn_oracle_main'

task_BCU_CUST_TNDC_INF_TO_HDHS_QEURY = f"""WHERE TO_DATE(SMR_DT, 'YYYYMMDD') BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD')
"""

task_CU_CUST_MKTG_MST_TO_HDHS_QEURY = f"""WHERE MKTG_RFS_TRGT_YN = 'Y' 
AND TO_DATE(NOTC_PRRG_DT, 'YYYYMMDD') 
BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD') + 1
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD') + 1
"""


task_CU_CUST_STAT_DTL_TO_HDHS_QUERY = f"""WHERE (ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS')))
OR (VLID_TERM_EXPY_PRRG_DT BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD'))
"""


task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS_QEURY = f"""WHERE TO_DATE(NOTC_PRRG_DT, 'YYYYMMDD') 
BETWEEN DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')) 
AND DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD')) 
AND ETL_DTM BETWEEN TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 0, 0, 0) 
AND TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 23, 59, 59)
"""

task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

REAL_SWRT_QEURY = f"""WHERE APLY_DT >= TO_CHAR(DATEADD(DAY, 1,TO_DATE('{var.daily_main_p_start}','YYYYMMDD')),'YYYYMMDD') 
AND APLY_DT <= TO_CHAR(DATEADD(DAY, 1,TO_DATE('{var.daily_main_p_end}','YYYYMMDD')),'YYYYMMDD')
"""

HES_RNTL_ARLT_DTL_TO_HDHS_QUERY = f"""WHERE CHG_DTM BETWEEN TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS')
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

ETL_DTM_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

HES_RNTL_ARLT_DTL_QUERY = f"""WHERE CHG_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

task_CU_MKTG_AGR_SMS_DTL_TO_HDHS_QUERY = f"""WHERE REG_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

RAR_SALE_QUERY = f"""WHERE ETL_DTM BETWEEN TO_TIMESTAMP('{var.daily_main_p_start}' || '060000', 'YYYYMMDDHH24MISS')
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '055959', 'YYYYMMDDHH24MISS'))
"""

task_OM_AFCR_ORD_ARLT_SMR_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

task_OM_SCWD_DLU_ORD_ARLT_SMR_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_DAILY_TO_HDHS",
    schedule_interval=None,
    catchup=False,
    # start_date=pendulum.datetime(2025, 3, 9, tz="Asia/Seoul"),
    tags=["현대홈쇼핑","ODS","역방향","Scheduled"]
) as dag:

    task_RAR_REAL_SWRT_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWRT_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWRT_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWRT_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWRT_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
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

    task_BCU_CUST_TNDC_INF_TO_HDHS = PythonOperator(
        task_id="task_BCU_CUST_TNDC_INF_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['COLUMNS'],
                 var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['PK_COLUMNS'],
                 task_BCU_CUST_TNDC_INF_TO_HDHS_QEURY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_HES_RNTL_ARLT_DTL_TO_HDHS = PythonOperator(
        task_id="task_HES_RNTL_ARLT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_HES_RNTL_ARLT_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_RNTL_ARLT_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_RNTL_ARLT_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_HES_RNTL_ARLT_DTL_TO_HDHS']['PK_COLUMNS'],
                 HES_RNTL_ARLT_DTL_TO_HDHS_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )


    task_BOD_ORD_DTL_TO_HDHS = PythonOperator(
        task_id="task_BOD_ORD_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_BOD_ORD_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_BOD_ORD_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_BOD_ORD_DTL_TO_HDHS']['COLUMNS'],
                 var_dict['task_BOD_ORD_DTL_TO_HDHS']['PK_COLUMNS'],
                 ETL_DTM_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RDM_SELL_MDA_DIM_TO_HDHS = PythonOperator(
        task_id="task_RDM_SELL_MDA_DIM_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['COLUMNS'],
                 var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['PK_COLUMNS'],
                 ""],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )


    task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS = PythonOperator(
        task_id="task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['COLUMNS'],
                 var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['PK_COLUMNS'],
                 ""],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_SALE_REAL_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_SALE_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_SALE_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_SALE_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_SALE_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_SALE_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS= PythonOperator(
        task_id="task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_MDA_SALE_REAL_SMR_TO_HDHS= PythonOperator(
        task_id="task_RAR_MDA_SALE_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_MDA_SALE_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_MDA_SALE_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_MDA_SALE_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_MDA_SALE_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS= PythonOperator(
        task_id="task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_BITM_SELL_REAL_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_BITM_SELL_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_BITM_SELL_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_BITM_SELL_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_BITM_SELL_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_BITM_SELL_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS = PythonOperator(
        task_id="task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS']['COLUMNS'],
                 var_dict['task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS']['PK_COLUMNS'],
                 RAR_SALE_QUERY],
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_OM_AFCR_ORD_ARLT_SMR = PythonOperator(
        task_id="task_OM_AFCR_ORD_ARLT_SMR",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_OM_AFCR_ORD_ARLT_SMR']['SNOW_TABLE'],
                 var_dict['task_OM_AFCR_ORD_ARLT_SMR']['ORA_MAIN_TABLE'],
                 var_dict['task_OM_AFCR_ORD_ARLT_SMR']['COLUMNS'],
                 var_dict['task_OM_AFCR_ORD_ARLT_SMR']['PK_COLUMNS'],
                 task_OM_AFCR_ORD_ARLT_SMR_QUERY],
        provide_context=True,
        # on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    task_OM_SCWD_DLU_ORD_ARLT_SMR = PythonOperator(
        task_id="task_OM_SCWD_DLU_ORD_ARLT_SMR",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id,
                 ora_main_conn_id,
                 var_dict['task_OM_SCWD_DLU_ORD_ARLT_SMR']['SNOW_TABLE'],
                 var_dict['task_OM_SCWD_DLU_ORD_ARLT_SMR']['ORA_MAIN_TABLE'],
                 var_dict['task_OM_SCWD_DLU_ORD_ARLT_SMR']['COLUMNS'],
                 var_dict['task_OM_SCWD_DLU_ORD_ARLT_SMR']['PK_COLUMNS'],
                 task_OM_SCWD_DLU_ORD_ARLT_SMR_QUERY],
        provide_context=True,
        # on_failure_callback=notify_api_on_error,
        trigger_rule="all_done"
    )

    [task_BOD_ORD_DTL_TO_HDHS, task_RDM_SELL_MDA_DIM_TO_HDHS, task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS] >> \
    task_OM_AFCR_ORD_ARLT_SMR >> \
    task_OM_SCWD_DLU_ORD_ARLT_SMR >> \
    task_RAR_REAL_SWRT_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS >> \
    task_CU_CUST_MKTG_AGR_MST_TO_HDHS >> \
    task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS >> \
    task_BCU_CUST_TNDC_INF_TO_HDHS >> \
    task_HES_RNTL_ARLT_DTL_TO_HDHS >> \
    task_RAR_SALE_REAL_SMR_TO_HDHS >> \
    task_RAR_ITEM_SALE_REAL_SMR_TO_HDHS >> \
    task_RAR_MDA_SALE_REAL_SMR_TO_HDHS >> \
    task_RAR_BITM_MDA_SELL_REAL_SMR_TO_HDHS >> \
    task_RAR_BITM_MDA_SELL_ETC_SMR_TO_HDHS >> \
    task_RAR_BITM_SELL_PTC_REAL_SMR_TO_HDHS >> \
    task_RAR_BITM_SELL_REAL_SMR_TO_HDHS >> \
    task_RAR_SELL_PTC_ETC_REAL_SMR_TO_HDHS >> \
    task_RAR_BITM_SELL_ETC_REAL_SMR_TO_HDHS



