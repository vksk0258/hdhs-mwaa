from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
import boto3
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import pendulum

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

        temp_table = f"{ora_table_name}{chunk_index}"
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

CONDITION_QUERY = f"""WHERE CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{p_end}' || '235959', 'YYYYMMDDHH24MISS')
"""

CONDITION_QUERY2 = f"""WHERE CHG_DTM >= TO_DATE('{p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{p_end}' || '235959', 'YYYYMMDDHH24MISS')
"""

# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_WEEKLY_TO_HDHS",
    schedule_interval=None,
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:

    task_HES_EXP_SWRT_DTL_TO_HDHS = PythonOperator(
        task_id="task_HES_EXP_SWRT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['SNOW_TABLE'], var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['MAIN_TABLE'],
                 var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY]
    )

    task_HES_EXP_SWRT_ETC_DTL_TO_HDHS= PythonOperator(
        task_id="task_HES_EXP_SWRT_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['MAIN_TABLE'],
                 var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY]
    )

    task_HES_EXP_SWRT_MLB_DTL_TO_HDHS= PythonOperator(
        task_id="task_HES_EXP_SWRT_MLB_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['MAIN_TABLE'],
                 var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_MLB_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY]
    )

    task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS= PythonOperator(
            task_id="task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS",
            python_callable=snow_to_snow_merge,
            op_args=[snow_conn_id, ora_conn_id, var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['SNOW_TABLE'],
                     var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['MAIN_TABLE'],
                     var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS']['PK_COLUMNS'],
                     CONDITION_QUERY]
        )

    task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS= PythonOperator(
            task_id="task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS",
            python_callable=snow_to_snow_merge,
            op_args=[snow_conn_id, ora_conn_id, var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['SNOW_TABLE'],
                     var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['MAIN_TABLE'],
                     var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                     CONDITION_QUERY]
        )

    task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS = PythonOperator(
        task_id="task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[snow_conn_id, ora_conn_id, var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['SNOW_TABLE'],
                 var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['MAIN_TABLE'],
                 var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS']['PK_COLUMNS'],
                 CONDITION_QUERY]
    )

    task_HES_EXP_SWRT_DTL_TO_HDHS >> task_HES_EXP_SWRT_ETC_DTL_TO_HDHS >> task_HES_EXP_SWRT_MLB_DTL_TO_HDHS >> \
    task_HES_EXP_SWRT_ONLN_DTL_TO_HDHS >> task_HES_EXP_SWRT_ONLN_ETC_DTL_TO_HDHS >> task_RCU_UPNT_JOIN_RATE_DTL_TO_HDHS
