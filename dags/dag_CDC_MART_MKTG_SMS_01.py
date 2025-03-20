from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from common.notify_error_functions import notify_api_on_error
from airflow.decorators import task
import pandas as pd
import boto3
import json
from airflow.models import Variable
import pendulum

KST = pendulum.timezone("Asia/Seoul")

var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')

var_dict = json.loads(var_text)

client_path = Variable.get("client_path")

# S3 parameters
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_1200_CP_SMR_MST_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

task_CU_MKTG_AGR_SMS_DTL_TO_HDHS_QUERY = f"""WHERE REG_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

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


with DAG(
    dag_id="dag_CDC_MART_MKTG_SMS_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","dag_DD01_1030_MKTG_AGR_SMS_01", "MART프로시져"]
) as dag:
    task_SP_BMK_CUST_MKTG_AGR_HIS = PythonOperator(
        task_id="task_SP_BMK_CUST_MKTG_AGR_HIS",
        python_callable=execute_procedure,
        op_args=["SP_BMK_CUST_MKTG_AGR_HIS", p_start, p_end, 'conn_snowflake_insu'],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    @task(task_id='task_CU_MKTG_AGR_SMS_DTL_TO_HDHS', provide_context=True,
          trigger_rule="all_done")
    def task_CU_MKTG_AGR_SMS_DTL_TO_HDHS(**kwargs):

        snow_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_insu')
        ora_main_hook = OracleHook(oracle_conn_id=ora_main_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
        ora_main_connection = ora_main_hook.get_conn()

        snow_table = var_dict['task_CU_MKTG_AGR_SMS_DTL_TO_HDHS']['SNOW_TABLE']
        ora_main_table = var_dict['task_CU_MKTG_AGR_SMS_DTL_TO_HDHS']['ORA_MAIN_TABLE']
        columns = var_dict['task_CU_MKTG_AGR_SMS_DTL_TO_HDHS']['COLUMNS']
        pk_columns = var_dict['task_CU_MKTG_AGR_SMS_DTL_TO_HDHS']['PK_COLUMNS']

        with snow_hook.get_conn() as snow_connection:
            query = f"""
                                SELECT *
                                FROM {snow_table}
                             """
            query += f"""{task_CU_MKTG_AGR_SMS_DTL_TO_HDHS_QUERY}
                            """

            print("==================[ORACLE QEURY]==================")
            print(query)
            df = pd.read_sql(query, snow_connection)
            print(f"## {snow_table} 조회 카운트 : {len(df)}")
            print(df.head(1))

            df_tf = pd.DataFrame(index=df.index)

            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['CUST_NM'] = df['CUST_NM']
            df_tf['TEL'] = df['TEL']
            df_tf['SEND_CNTN'] = df['SEND_CNTN']
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
                batch = df_tf[i: i + batch_size]
                ora_main_cursor.executemany(insert_query, batch)
                ora_main_connection.commit()
                print(f"총{len(df)}건 중 {i + 10000}건 커밋 완료")

            ora_main_cursor.execute(f"SELECT COUNT(*) FROM {ora_main_table}")
            print("######TMP에 들어간 건수######")
            print(ora_main_cursor.fetchone())

        ora_main_connection.close()
        print("커넥션 종료")


    # task_SP_CU_MKTG_AGR_SMS_SND_P = PythonOperator(
    #     task_id="task_SP_CU_MKTG_AGR_SMS_SND_P",
    #     python_callable=execute_procedure_dycl,
    #     op_args=["SP_CU_MKTG_AGR_SMS_SND_P", p_start, p_end, 'conn_snowflake_insu'],
    #     trigger_rule="all_done",
    #     provide_context=True,
    #     on_failure_callback=notify_api_on_error
    # )

    task_CU_MKTG_AGR_SMS_DTL_TO_HDHS = task_CU_MKTG_AGR_SMS_DTL_TO_HDHS()

    task_SP_BMK_CUST_MKTG_AGR_HIS >> task_CU_MKTG_AGR_SMS_DTL_TO_HDHS