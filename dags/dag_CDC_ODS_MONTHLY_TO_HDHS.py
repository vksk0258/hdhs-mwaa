from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from common.notify_error_functions import notify_api_on_error
from airflow.decorators import task
from decimal import Decimal
from datetime import datetime, timedelta
import boto3
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import pendulum

KST = pendulum.timezone("Asia/Seoul")

var_text = Variable.get('VAR_dag_CDC_ODS_MONTHLY_TO_HDHS')
var_dict = json.loads(var_text)

client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0030_MONTHLY_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

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

            truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('HDHS_DW','{ora_table_name}_TEMP')"
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

            # 숫자형 컬럼 변환 (NaN -> None, float -> Decimal)
            for col in df.select_dtypes(include=['number']).columns:
                df[col] = df[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # 날짜 컬럼 리스트
            date_columns = df.select_dtypes(include=['datetime', 'datetimetz']).columns.tolist()

            # INSERT 쿼리 생성
            insert_query = f"""
                INSERT INTO {ora_main_table}_TEMP ({", ".join(columns)})
                VALUES ({", ".join([f":{i + 1}" for i in range(len(columns))])})
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

CONDITION_QUERY = f"""WHERE CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') + 1
AND CHG_DTM <= TO_DATE('{p_end}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

CONDITION_QUERY2 = f"""WHERE CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') + 1
AND CHG_DTM <= TO_DATE('{p_end}' || '235959', 'YYYYMMDDHH24MISS') + 2
"""

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_MONTHLY_TO_HDHS",
    schedule_interval=None,
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:
    @task.branch(task_id='branching')
    def check_monthly(p_end):
        if p_end[6:8] == '01': # 7번째(인덱스 6)부터 2글자
            return ['task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M',
                    'task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV',
                    'task_TMP_GOLD_CUST_NO_TO_HDHS',
                    'task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql',
                    'task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01'
                    ]
        elif (datetime.strptime(p_end, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")[6:8] == '01':
            return 'task_IM_VEN_CNTB_RATE_SMR_TO_HDHS'

    task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M = PythonOperator(
        task_id="task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M",
        python_callable=execute_procedure,
        op_args=["SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error

    )
    @task(task_id='task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql',
          trigger_rule="none_skipped",
          provide_context=True,
          on_failure_callback=notify_api_on_error
          )
    def task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql(p_start,p_end,ora_main_conn_id):
        pre_sql = f"""
        DELETE FROM HDHS_DW.CU_TCS_CUST_HIS_MERGE
        WHERE BSIC_YR || BSIC_MM 
        BETWEEN TO_CHAR(TO_DATE('{p_start}', 'YYYYMMDD'), 'YYYYMM') 
        AND TO_CHAR(TO_DATE('{p_end}', 'YYYYMMDD'), 'YYYYMM')"""
        print(pre_sql)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        with ora_main_hook.get_conn() as ora_main_connection:
            with ora_main_connection.cursor() as ora_main_cursor:
                ora_main_cursor.execute(pre_sql)
                print("#####SQL 수행#####")
                print(ora_main_cursor.fetchone)


    task_CU_TCS_CUST_HIS_TO_HDHS = PythonOperator(
        task_id="task_CU_TCS_CUST_HIS_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_CU_TCS_CUST_HIS_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_CU_TCS_CUST_HIS_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_CU_TCS_CUST_HIS_TO_HDHS']['COLUMNS'], var_dict['task_CU_TCS_CUST_HIS_TO_HDHS']['PK_COLUMNS'],
                 ""],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error

    )

    task_IM_VEN_CNTB_RATE_SMR_TO_HDHS = PythonOperator(
        task_id="task_IM_VEN_CNTB_RATE_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_IM_VEN_CNTB_RATE_SMR_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_IM_VEN_CNTB_RATE_SMR_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_IM_VEN_CNTB_RATE_SMR_TO_HDHS']['COLUMNS'], var_dict['task_IM_VEN_CNTB_RATE_SMR_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error

    )

    task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS = PythonOperator(
        task_id="task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS']['COLUMNS'], var_dict['task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY2],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error

    )

    task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS = PythonOperator(
        task_id="task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS']['COLUMNS'], var_dict['task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY2],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error

    )

    task_HES_BRND_CTPF_RATE_DTL_TO_HDHS = PythonOperator(
        task_id="task_HES_BRND_CTPF_RATE_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_HES_BRND_CTPF_RATE_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_BRND_CTPF_RATE_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_BRND_CTPF_RATE_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_BRND_CTPF_RATE_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS = PythonOperator(
        task_id="task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_CTPF_RATE_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_CTPF_RATE_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_RAR_CTPF_RATE_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_CTPF_RATE_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY2],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY2],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge2,
        op_args=[snow_conn_id, ora_main_conn_id, var_dict['task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS']['ORA_MAIN_TABLE'],
                 var_dict['task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY2],
        trigger_rule="none_skipped",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    @task(task_id='task_TMP_GOLD_CUST_NO_DW_TO_HDHS',
          trigger_rule="none_skipped",
          provide_context=True)
    def task_TMP_GOLD_CUST_NO_DW_TO_HDHS():
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_TMP_GOLD_CUST_NO_DW_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_TMP_GOLD_CUST_NO_DW_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_TMP_GOLD_CUST_NO_DW_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_TMP_GOLD_CUST_NO_DW_TO_HDHS']['PK_COLUMNS']


        with snow_hook.get_conn() as snow_connection:

            query = f"""
                            SELECT *
                            FROM {snow_table}
                         """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df_tf = pd.DataFrame(index=df.index)

            df_tf['STAD_MM'] = df['BSIC_YM']
            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['PROC_DTM'] = df['WRK_DTM']

        with ora_main_connection.cursor() as ora_main_cursor:
            ora_schema, ora_table_name = ora_main_table.split('.')

            # truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            # print("==================[TRUNCATE QEURY]==================")
            # print(truncate_query)
            # ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].astype(float).where(df_tf[col].notnull(), None)

            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

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
            df_tf = df_tf.apply(lambda row: [
                row[col].to_pydatetime() if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            print("==================[MERRGE QUERY]==================")
            print(merge_query)

            # BATCH MERGE 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()

                print(f"총{len(df_tf)}건 중 {i + 10000}건 커밋 완료")

        ora_main_connection.close()
        print("커넥션 종료")


    @task(task_id='task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV',
          trigger_rule="none_skipped",
          provide_context=True)
    def task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV(p_start):
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV']['SNOW_TABLE']
        ora_main_table = var_dict['task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV']['ORA_MAIN_TABLE']
        columns = var_dict['task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV']['COLUMNS']
        pk_columns = var_dict['task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV']['PK_COLUMNS']


        with snow_hook.get_conn() as snow_connection:
            query = f"""
                            SELECT *
                            FROM {snow_table}
                         """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                    else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                )

            df.replace({np.nan: None}, inplace=True)

            print(df.head)

            print(p_start)

            df_tf = pd.DataFrame(index=df.index)

            df_tf['BSIC_YM'] = p_start[:6]
            df_tf['CUST_NO'] = df['CUST_NO']
            # 변환 매핑 딕셔너리 생성
            decode_mapping = {
                'F': '1001',
                'FC': '1001',
                'A': '2001',
                'M': '3001',
                'I': '4001',
                'L': '5001',
                'Y': '6001',
                'T': '7001',
                'X': None  # 'X'는 변환값이 없으므로 None 처리
            }
            # df_tf['ITNT_GRD_GBCD']에 변환된 값 저장
            df_tf['ITNT_GRD_GBCD'] = df['HMALL_CUST_GRD_GBCD'].map(decode_mapping)
            df_tf['BLOG_CNT'] = df['ITEM_EVAL_CNT']
            df_tf['ORD_CNT'] = df['RORD_CNT']
            df_tf['ORD_AMT'] = df['RORD_AMT']
            df_tf['RCNT_11_MTHS_EVAL_CNT'] = df['M11_ITEM_EVAL_CNT']
            df_tf['RCNT_11_MTHS_ORD_CNT'] = df['M11_RORD_CNT']
            df_tf['RCNT_11_MTHS_ORD_AMT'] = df['M11_RORD_AMT']
            df_tf['GRD_RETN_MTHS'] = df['GRD_RETN_MTHS']
            df_tf['RGST_ID'] = "1404268"
            df_tf['RGST_IP'] = "SYSTEM"
            df_tf['REG_DTM'] = pendulum.now(KST).strftime('%Y-%m-%d %H:%M:%S')
            df_tf['CHGP_ID'] = "1404268"
            df_tf['CHGP_IP'] = "SYSTEM"
            df_tf['CHG_DTM'] = pendulum.now(KST).strftime('%Y-%m-%d %H:%M:%S')

            print(df_tf.head)
            print(len(df_tf))

        with ora_main_connection.cursor() as ora_main_cursor:
            ora_schema, ora_table_name = ora_main_table.split('.')

            # truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            # print("==================[TRUNCATE QEURY]==================")
            # print(truncate_query)
            # ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].astype(float).where(df_tf[col].notnull(), None)

            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

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
            df_tf = df_tf.apply(lambda row: [
                row[col].to_pydatetime() if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            print("==================[MERRGE QUERY]==================")
            print(merge_query)

            # BATCH MERGE 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()

                print(f"총{len(df_tf)}건 중 {i + 10000}건 커밋 완료")

        ora_main_connection.close()
        print("커넥션 종료")


    @task(task_id='task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01',
          trigger_rule="none_skipped",
          provide_context=True)
    def task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01(p_start):
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01']['SNOW_TABLE']
        ora_main_table = var_dict['task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01']['ORA_MAIN_TABLE']
        columns = var_dict['task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01']['COLUMNS']
        pk_columns = var_dict['task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01']['PK_COLUMNS']

        ora_schema, ora_table_name = ora_main_table.split('.')

        with snow_hook.get_conn() as snow_connection:

            query = f"""
                            SELECT *
                            FROM {snow_table}
                         """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df.replace({np.nan: None}, inplace=True)

            print(df.head)
            print(p_start)

            df_tf = pd.DataFrame(index=df.index)

            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['BSIC_YM'] = p_start[:6]
            df_tf['PET_GRD_GBCD'] = df['PET_GRD_GBCD']
            df_tf['RGST_ID'] = "2100374"
            df_tf['RGST_IP'] = "DW_SYSTEM"
            df_tf['REG_DTM'] = pendulum.now(KST)
            df_tf['CHGP_ID'] = "2100374"
            df_tf['CHGP_IP'] = "DW_SYSTEM"
            df_tf['CHG_DTM'] = pendulum.now(KST)

            print(df_tf.head)
            print(len(df_tf))

        with ora_main_connection.cursor() as ora_main_cursor:
            ora_schema, ora_table_name = ora_main_table.split('.')

            # truncate_query = f"CALL HDHS.SP_CM_TRUNC_TB('{ora_schema}','{ora_table_name}')"
            # print("==================[TRUNCATE QEURY]==================")
            # print(truncate_query)
            # ora_main_cursor.execute(truncate_query)

            # 숫자형 컬럼 변환 (NaN을 None으로 변환)
            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].astype(float).where(df_tf[col].notnull(), None)

            for col in df_tf.select_dtypes(include=['number']).columns:
                df_tf[col] = df_tf[col].apply(lambda x: Decimal(str(x)) if pd.notnull(x) else None)

            # 날짜 컬럼 유지 및 변환하지 않음
            date_columns = df_tf.select_dtypes(include=['datetime', 'datetimetz']).columns

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
            df_tf = df_tf.apply(lambda row: [
                row[col].to_pydatetime() if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            print("==================[MERRGE QUERY]==================")
            print(merge_query)

            # BATCH MERGE 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()

                print(f"총{len(df_tf)}건 중 {i + 10000}건 커밋 완료")

        ora_main_connection.close()
        print("커넥션 종료")

    task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV = task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV(p_start)
    task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01 = task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01(p_start)
    task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql = task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql(p_start,p_end,ora_main_conn_id)
    task_TMP_GOLD_CUST_NO_TO_HDHS = task_TMP_GOLD_CUST_NO_DW_TO_HDHS()


    check_monthly(p_end) >> [task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M,
     task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV,
     task_TMP_GOLD_CUST_NO_TO_HDHS,
     task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql,
     task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01,
     task_IM_VEN_CNTB_RATE_SMR_TO_HDHS]

    task_CU_TCS_CUST_HIS_TO_HDHS_pre_sql >> task_CU_TCS_CUST_HIS_TO_HDHS

    task_IM_VEN_CNTB_RATE_SMR_TO_HDHS >> task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS >> \
    task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS >> task_HES_BRND_CTPF_RATE_DTL_TO_HDHS >> \
    task_HES_BRND_CTPF_RATE_ETC_DTL_TO_HDHS >> task_HES_DRCT_PRCH_LOSS_DTL_TO_HDHS >> \
    task_RAR_CTPF_RATE_DTL_TO_HDHS >> task_RAR_CTPF_RATE_ETC_DTL_TO_HDHS >> \
    task_RAR_CTPF_RATE_HMALL_DTL_TO_HDHS



