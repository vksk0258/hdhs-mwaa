from airflow import DAG
from airflow.operators.python import PythonOperator
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
import pandas as pd
import boto3
from airflow.models import Variable
from common.notify_error_functions import notify_api_on_error
from airflow.decorators import task
import json
import pendulum

# Load parameters from S3
s3 = boto3.client('s3')
PARAM_BUCKET_NAME = "hdhs-dw-mwaa-s3"
PARAM_KEY = "param/wf_DD01_0030_DAILY_MAIN_01.json"
response = s3.get_object(Bucket=PARAM_BUCKET_NAME, Key=PARAM_KEY)
params = json.load(response['Body'])

KST = pendulum.timezone("Asia/Seoul")

var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')

var_dict = json.loads(var_text)

client_path = Variable.get("client_path")

# Parse time parameters
p_start = params.get('$$P_START')
p_end = params.get('$$P_END')

snow_conn_id = 'conn_snowflake_etl'
ora_main_conn_id = 'conn_oracle_main'

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
    type = "daily"
    schema_name = ora_schema
    table_name = ora_table_name
    mwaa_cnt = len(df)
    mwaa_st = pendulum.now(KST)
    reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)

task_CU_CUST_STAT_DTL_TO_HDHS_QUERY = f"""WHERE (ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{p_end}' || '235959', 'YYYYMMDDHH24MISS')))
OR (VLID_TERM_EXPY_PRRG_DT BETWEEN TO_DATE('{p_start}', 'YYYYMMDD')
AND TO_DATE('{p_end}', 'YYYYMMDD'))
"""

task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_VLID_TERM_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","MART프로시져"]
) as dag:
    task_SP_TRUNCATE_FOR_DW = PythonOperator(
        task_id="task_SP_TRUNCATE_FOR_DW",
        python_callable=execute_procedure,
        op_args=["HDHS_NSTD.SP_TRUNCATE_FOR_DW", p_start, p_end],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    @task(task_id='task_CU_CUST_STAT_DTL_TO_HDHS', provide_context=True)
    def task_CU_CUST_STAT_DTL_TO_HDHS(**kwargs):
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path,
                                   trigger_rule="all_done")
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:

            query = f"""
                            SELECT *
                            FROM {snow_table}
                         """
            query += f"""{task_CU_CUST_STAT_DTL_TO_HDHS_QUERY}
                        """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            # 인덱스가 0부터 26까지 총 27개 있는 빈 데이터프레임 생성
            df_tf = pd.DataFrame(index=df.index)

            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['CUST_STAT_GBCD'] = df['CUST_STAT_GBCD']
            df_tf['NOTC_PRRG_DT'] = df['NOTC_PRRG_DT']
            df_tf['NOTC_CMPT_DT'] = None
            df_tf['VLID_TERM_EXPY_PRRG_DT'] = df['VLID_TERM_EXPY_PRRG_DT']
            df_tf['VLID_TERM_MVOT_CMPT_DT'] = df['VLID_TERM_MVOT_CMPT_DT']
            df_tf['DROT_PRRG_DT'] = df['DROT_PRRG_DT']
            df_tf['DROT_CMPT_DT'] = df['DROT_CMPT_DT']
            df_tf['CUST_REST_DT'] = df['CUST_REST_DT']
            df_tf['LAST_TRD_DT'] = df['LAST_TRD_DT']
            df_tf['LAST_ACSS_DT'] = df['LAST_ACSS_DT']
            df_tf['LAST_CTI_ACSS_DT'] = df['LAST_CTI_ACSS_DT']
            df_tf['LAST_ONLN_ACSS_DT'] = df['LAST_ONLN_ACSS_DT']
            df_tf['LAST_INSU_CONT_DT'] = df['LAST_INSU_CONT_DT']
            df_tf['LAST_DATA_BROD_ORD_DT'] = df['LAST_DATA_BROD_ORD_DT']
            df_tf['FRST_CUST_REG_DT'] = df['FRST_CUST_REG_DT']
            df_tf['LAST_ORD_DT'] = df['LAST_ORD_DT']
            df_tf['LAST_EVNT_ENTRY_DT'] = df['LAST_EVNT_ENTRY_DT']
            df_tf['LAST_SVMT_USE_DT'] = df['LAST_SVMT_USE_DT']
            df_tf['LAST_CDPST_USE_DT'] = df['LAST_CDPST_USE_DT']
            df_tf['LAST_DATA_BROD_ACSS_DT'] = df['LAST_DATA_BROD_ACSS_DT']
            df_tf['RGST_ID'] = "SYSTEM"
            df_tf['RGST_IP'] = "192.168.20.176"
            df_tf['REG_DTM'] = df['REG_DTM']
            df_tf['CHGP_ID'] = "SYSTEM"
            df_tf['CHGP_IP'] = "192.168.20.176"
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

            # MERGE Query 생성
            set_clause = ", ".join([
                f"{col} = TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f"{col} = :{i + 1}"
                for i, col in enumerate(columns) if col not in pk_columns
            ])
            insert_values = ", ".join([
                f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}"
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

            print("==================[MERGE Query]==================")
            print(merge_query)

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df_tf = df_tf.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######총 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")

        task_name = kwargs['ti'].task_id[5:]
        type = "daily"
        schema_name = ora_schema
        table_name = ora_table_name
        mwaa_cnt = len(df)
        mwaa_st = pendulum.now(KST)
        reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)

    @task(task_id='task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS', provide_context=True,
        trigger_rule="all_done")
    def task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS(**kwargs):

        snow_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_insu')
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:
            query = f"""
                            SELECT *
                            FROM {snow_table}
                         """
            query += f"""{task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_QUERY}
                        """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df_tf = pd.DataFrame(index=df.index)

            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['EMAIL_ADR'] = df['EMAIL_ADR']
            df_tf['NOTC_PRRG_DT'] = df['NOTC_PRRG_DT']
            df_tf['NOTC_CMPT_DT'] = df['NOTC_CMPT_DT']
            df_tf['VLID_TERM_EXPY_PRRG_DT'] = df['VLID_TERM_EXPY_PRRG_DT']
            df_tf['RGST_ID'] = "SYSTEM"
            df_tf['RGST_IP'] = "192.168.20.176"
            df_tf['REG_DTM'] = df['REG_DTM']
            df_tf['CHGP_ID'] = "SYSTEM"
            df_tf['CHGP_IP'] = "192.168.20.176"
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

            # MERGE Query 생성
            set_clause = ", ".join([
                f"{col} = TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f"{col} = :{i + 1}"
                for i, col in enumerate(columns) if col not in pk_columns
            ])
            insert_values = ", ".join([
                f"TO_TIMESTAMP(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if col in date_columns else f":{i + 1}"
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

            print("==================[MERGE Query]==================")
            print(merge_query)

            # executemany 실행 전에 데이터 타입 변환 (날짜 컬럼만 문자열로 변환)
            df_tf = df_tf.apply(lambda row: [
                row[col].strftime('%Y-%m-%d %H:%M:%S') if col in date_columns and pd.notnull(row[col]) else row[col]
                for col in columns
            ], axis=1).tolist()

            # BATCH INSERT 수행
            batch_size = 10000
            for i in range(0, len(df_tf), batch_size):
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(merge_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######총 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")

        task_name = kwargs['ti'].task_id[5:]
        type = "daily"
        schema_name = ora_schema
        table_name = ora_table_name
        mwaa_cnt = len(df)
        mwaa_st = pendulum.now(KST)
        reverse_verfi(task_name, type, schema_name, table_name, mwaa_cnt, mwaa_st)


    task_SP_HDHS_CU_CUST_STAT_DTL_DW = PythonOperator(
        task_id="task_SP_HDHS_CU_CUST_STAT_DTL_DW",
        python_callable=execute_procedure,
        op_args=["HDHS_CU.SP_HDHS_CU_CUST_STAT_DTL_DW", p_start, p_end,"conn_oracle_main"],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_CU_CUST_STAT_DTL_TO_HDHS = task_CU_CUST_STAT_DTL_TO_HDHS()
    task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS = task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS()

    task_SP_TRUNCATE_FOR_DW >> task_CU_CUST_STAT_DTL_TO_HDHS >> [task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS , task_SP_HDHS_CU_CUST_STAT_DTL_DW]