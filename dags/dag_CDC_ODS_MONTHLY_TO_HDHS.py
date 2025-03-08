from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.common_call_procedure import execute_procedure, execute_procedure_dycl, log_etl_completion
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from airflow.decorators import task
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

p_start = "20250228"
p_end = "20250228"

# p_start = params.get("$$P_START")
# p_end = params.get("$$P_END")

def snow_to_snow_merge(snow_conn_id, ora_conn_id, snow_table, ora_table, columns, pk_columns, condition_query):
    import os
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    snow_schema, snow_table_name = snow_table.split('.')
    ora_schema, ora_table_name = ora_table.split('.')

    snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
    oracle_hook = SnowflakeHook(snowflake_conn_id=ora_conn_id)
    ora_connection = oracle_hook.get_conn()

    chunk_index = 1

    with snow_hook.get_conn() as snow_connection:

        query = f"""
                    SELECT {', '.join(columns)}
                    FROM {snow_table}
                 """
        query += f"""{condition_query}
                """

        print("==================[SNOWFLAKE QEURY]==================")
        print(query)
        df = pd.read_sql(query, snow_connection)
        print(f"## {snow_table} 조회 카운트 : {len(df)}")

        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        # NaN 값을 명확하게 None으로 변환
        df.replace({np.nan: None}, inplace=True)

        with ora_connection.cursor() as ora_cursor:


            # MERGE 쿼리 생성 (배치 처리)
            values = df.where(pd.notnull(df), None).values.tolist()

            batch_size = 10000  # 한 번에 실행할 최대 행 수

            for i in range(0, len(values), batch_size):

                temp_table = f"{ora_table_name}{chunk_index}"

                create_temp_table_query = f"""
                                        CREATE TEMPORARY TABLE {ora_schema}.{temp_table} AS
                                        SELECT * FROM {ora_table} WHERE 1=0;
                                        """  # 빈 임시 테이블 생성
                print("==================[create_temp_table_query]==================")
                print(create_temp_table_query)

                ora_cursor.execute(create_temp_table_query)

                insert_query = f"""
                                INSERT INTO {ora_schema}.{temp_table} ({", ".join(columns)})
                                VALUES ({", ".join(["%s"] * len(columns))});
                                """
                print("==================[insert_query]==================")
                print(insert_query)



                batch = values[i: i + batch_size]

                ora_cursor.executemany(insert_query, batch)

                # 3️⃣ MERGE 실행
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

                update_set = ", ".join(
                    [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
                insert_columns = ", ".join(columns)
                insert_values = ", ".join([f"source.{col}" for col in columns])

                merge_query = f"""
                    MERGE INTO {ora_table} AS target
                    USING {ora_schema}.{temp_table} AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values});
                    """

                print("==================[merge_query]==================")
                print(merge_query)

                ora_cursor.execute(merge_query)

                print(f"{temp_table} 머지 완료!!!!")

                chunk_index += 1

    ora_connection.close()
    print("커넥션 종료")

snow_conn_id = 'conn_snowflake_etl'
ora_conn_id = 'conn_snow_load'

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
                    'task_CU_TCS_CUST_HIS_TO_HDHS',
                    'task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01'
                    ]
        elif (datetime.strptime(p_end, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")[6:8] == '01':
            return 'task_IM_VEN_CNTB_RATE_SMR_TO_HDHS'

    task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M = PythonOperator(
        task_id="task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M",
        python_callable=execute_procedure,
        op_args=["SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M", p_start, p_end, 'conn_snowflake_etl'],
        trigger_rule="none_skipped"
    )

    task_CU_TCS_CUST_HIS_TO_HDHS = PythonOperator(
        task_id="task_CU_TCS_CUST_HIS_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['CU_TCS_CUST_HIS']['SNOW_TABLE'],
                 var_dict['CU_TCS_CUST_HIS']['MAIN_TABLE'],
                 var_dict['CU_TCS_CUST_HIS']['COLUMNS'], var_dict['CU_TCS_CUST_HIS']['PK_COLUMNS'],
                 ""]
    )

    task_IM_VEN_CNTB_RATE_SMR_TO_HDHS = PythonOperator(
        task_id="task_IM_VEN_CNTB_RATE_SMR_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['IM_VEN_CNTB_RATE_SMR']['SNOW_TABLE'],
                 var_dict['IM_VEN_CNTB_RATE_SMR']['MAIN_TABLE'],
                 var_dict['IM_VEN_CNTB_RATE_SMR']['COLUMNS'], var_dict['IM_VEN_CNTB_RATE_SMR']['PK_COLUMNS'],
                 CONDITION_QUERY]
    )

    task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS = PythonOperator(
        task_id="task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['MK_AVG_WINT_INSM_CMIS_RATE']['SNOW_TABLE'],
                 var_dict['MK_AVG_WINT_INSM_CMIS_RATE']['MAIN_TABLE'],
                 var_dict['MK_AVG_WINT_INSM_CMIS_RATE']['COLUMNS'], var_dict['MK_AVG_WINT_INSM_CMIS_RATE']['PK_COLUMNS'],
                 CONDITION_QUERY2]
    )

    task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS = PythonOperator(
        task_id="task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['MK_AVG_WINT_INSM_CMIS_ETC_RATE']['SNOW_TABLE'],
                 var_dict['MK_AVG_WINT_INSM_CMIS_ETC_RATE']['MAIN_TABLE'],
                 var_dict['MK_AVG_WINT_INSM_CMIS_ETC_RATE']['COLUMNS'], var_dict['MK_AVG_WINT_INSM_CMIS_ETC_RATE']['PK_COLUMNS'],
                 CONDITION_QUERY2]
    )

    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              CONDITION_QUERY]
    # )

    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )
    #
    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )
    #
    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )
    #
    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )
    #
    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )
    #
    # task__TO_HDHS = PythonOperator(
    #     task_id="task__TO_HDHS",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[snow_conn_id, ora_conn_id, var_dict['']['SNOW_TABLE'],
    #              var_dict['']['MAIN_TABLE'],
    #              var_dict['']['COLUMNS'], var_dict['']['PK_COLUMNS'],
    #              ""]
    # )

    @task(task_id='task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV')
    def task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV(p_start):
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        oracle_hook = SnowflakeHook(snowflake_conn_id=ora_conn_id)
        oracle_conn = oracle_hook.get_conn()

        ora_schema = 'MWAA'
        ora_table = 'CU_ITNT_CUST_GRD_INF_TMP'

        query = "SELECT * FROM DW_RM.RCU_HMALL_CUST_GRD_DTL LIMIT 100"

        columns = [
            "BSIC_YM", "CUST_NO", "ITNT_GRD_GBCD", "BLOG_CNT", "ORD_CNT", "ORD_AMT",
            "RCNT_11_MTHS_EVAL_CNT", "RCNT_11_MTHS_ORD_CNT", "RCNT_11_MTHS_ORD_AMT",
            "GRD_RETN_MTHS", "RGST_ID", "RGST_IP", "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"
        ]

        pk_columns = ["BSIC_YM", "CUST_NO"]

        with snow_hook.get_conn() as snow_conn:
            df = pd.read_sql(query, snow_conn)

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

            chunk_index = 1

            with oracle_conn.cursor() as ora_cursor:

                # MERGE 쿼리 생성 (배치 처리)
                values = df_tf.where(pd.notnull(df_tf), None).values.tolist()

                batch_size = 10000  # 한 번에 실행할 최대 행 수

                for i in range(0, len(values), batch_size):

                    temp_table = f"{ora_schema}.{ora_table}{chunk_index}"

                    create_temp_table_query = f"""
                                       CREATE TEMPORARY TABLE {temp_table} AS
                                       SELECT * FROM {ora_schema}.{ora_table} WHERE 1=0;
                                       """  # 빈 임시 테이블 생성
                    print("==================[create_temp_table_query]==================")
                    print(create_temp_table_query)

                    ora_cursor.execute(create_temp_table_query)

                    insert_query = f"""
                                       INSERT INTO {temp_table} ({", ".join(columns)})
                                       VALUES ({", ".join(["%s"] * len(columns))});
                                       """
                    print("==================[insert_query]==================")
                    print(insert_query)


                    batch = values[i: i + batch_size]
                    ora_cursor.executemany(insert_query, batch)

                    # 3️⃣ MERGE 실행
                    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

                    update_set = ", ".join(
                        [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
                    insert_columns = ", ".join(columns)
                    insert_values = ", ".join([f"source.{col}" for col in columns])

                    merge_query = f"""
                               MERGE INTO {ora_schema}.{ora_table} AS target
                               USING {temp_table} AS source
                               ON {merge_condition}
                               WHEN MATCHED THEN
                                   UPDATE SET {update_set}
                               WHEN NOT MATCHED THEN
                                   INSERT ({insert_columns})
                                   VALUES ({insert_values});
                               """

                    print("==================[merge_query]==================")
                    print(merge_query)

                    ora_cursor.execute(merge_query)

                    chunk_index += 1

            oracle_conn.close()
            print("커넥션 종료")


    @task(task_id='task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01')
    def task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01(p_start):
        snow_hook = SnowflakeHook(snowflake_conn_id=snow_conn_id)
        oracle_hook = SnowflakeHook(snowflake_conn_id=ora_conn_id)
        oracle_conn = oracle_hook.get_conn()

        ora_schema = 'MWAA'
        ora_table = 'CU_ITNT_PET_CUST_GRD_INF_TMP'

        query = "SELECT * FROM DW_RM.ROD_CUST_PET_GRD_INF_TMP LIMIT 100"

        columns = [
            "CUST_NO", "BSIC_YM", "PET_GRD_GBCD", "RGST_ID", "RGST_IP",
            "REG_DTM", "CHGP_ID", "CHGP_IP", "CHG_DTM"
        ]

        pk_columns = ["BSIC_YM", "CUST_NO"]

        with snow_hook.get_conn() as snow_conn:
            df = pd.read_sql(query, snow_conn)

            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                    else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                )

            df.replace({np.nan: None}, inplace=True)

            print(df.head)

            print(p_start)

            df_tf = pd.DataFrame(index=df.index)

            df_tf['CUST_NO'] = df['CUST_NO']
            df_tf['BSIC_YM'] = p_start[:6]
            df_tf['PET_GRD_GBCD'] = df['PET_GRD_GBCD']
            df_tf['RGST_ID'] = "2100374"
            df_tf['RGST_IP'] = "DW_SYSTEM"
            df_tf['REG_DTM'] = pendulum.now(KST).strftime('%Y-%m-%d %H:%M:%S')
            df_tf['CHGP_ID'] = "2100374"
            df_tf['CHGP_IP'] = "DW_SYSTEM"
            df_tf['CHG_DTM'] = pendulum.now(KST).strftime('%Y-%m-%d %H:%M:%S')

            print(df_tf.head)
            print(len(df_tf))

            chunk_index = 1

            with oracle_conn.cursor() as ora_cursor:

                # MERGE 쿼리 생성 (배치 처리)
                values = df_tf.where(pd.notnull(df_tf), None).values.tolist()

                batch_size = 10000  # 한 번에 실행할 최대 행 수

                for i in range(0, len(values), batch_size):
                    temp_table = f"{ora_schema}.{ora_table}{chunk_index}"

                    create_temp_table_query = f"""
                                           CREATE TEMPORARY TABLE {temp_table} AS
                                           SELECT * FROM {ora_schema}.{ora_table} WHERE 1=0;
                                           """  # 빈 임시 테이블 생성
                    print("==================[create_temp_table_query]==================")
                    print(create_temp_table_query)

                    ora_cursor.execute(create_temp_table_query)

                    insert_query = f"""
                                           INSERT INTO {temp_table} ({", ".join(columns)})
                                           VALUES ({", ".join(["%s"] * len(columns))});
                                           """
                    print("==================[insert_query]==================")
                    print(insert_query)

                    batch = values[i: i + batch_size]
                    ora_cursor.executemany(insert_query, batch)

                    # 3️⃣ MERGE 실행
                    merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_columns])

                    update_set = ", ".join(
                        [f"target.{col} = source.{col}" for col in columns if col not in pk_columns])
                    insert_columns = ", ".join(columns)
                    insert_values = ", ".join([f"source.{col}" for col in columns])

                    merge_query = f"""
                                   MERGE INTO {ora_schema}.{ora_table} AS target
                                   USING {temp_table} AS source
                                   ON {merge_condition}
                                   WHEN MATCHED THEN
                                       UPDATE SET {update_set}
                                   WHEN NOT MATCHED THEN
                                       INSERT ({insert_columns})
                                       VALUES ({insert_values});
                                   """

                    print("==================[merge_query]==================")
                    print(merge_query)

                    ora_cursor.execute(merge_query)

                    chunk_index += 1

            oracle_conn.close()
            print("커넥션 종료")


    task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV = task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV(p_start)
    task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01 = task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01(p_start)


    check_monthly(p_end) >> [task_SP_BAR_ITEM_HNDL_ARLT_DLINE_DTL_M, task_CU_ITNT_CUST_GRD_INF_TMP_i_01_TO_HDHS_DEV, task_CU_TCS_CUST_HIS_TO_HDHS,task_CU_ITNT_PET_CUST_GRD_INF_TMP_TO_HDHS_i_01,task_IM_VEN_CNTB_RATE_SMR_TO_HDHS]

    task_IM_VEN_CNTB_RATE_SMR_TO_HDHS >> task_MK_AVG_WINT_INSM_CMIS_RATE_TO_HDHS >> task_MK_AVG_WINT_INSM_CMIS_ETC_RATE_TO_HDHS


