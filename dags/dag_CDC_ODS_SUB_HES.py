from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.python import PythonOperator
from common import reverse_task_variables as var
import json
import pendulum
import numpy as np
import pandas as pd
from airflow.models import Variable
import datetime

var_text = Variable.get('VAR_dag_CDC_ODS_SUB_HES')
client_path = Variable.get("client_path")

var_dict = json.loads(var_text)

def ora_to_snow_merge(etl_conn_id, load_conn_id, etl_table, load_table, columns, pk_columns, condition_query):
    import os
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    etl_schema, etl_table_name = etl_table.split('.')
    load_schema, load_table_name = load_table.split('.')


    oracle_hook = OracleHook(oracle_conn_id=etl_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
    load_hook = SnowflakeHook(snowflake_conn_id=load_conn_id)
    load_connection = load_hook.get_conn()

    chunk_index = 1

    with oracle_hook.get_conn() as etl_connection:

        temp_table = f"{load_table_name}{chunk_index}"
        query = f"""
                    SELECT {', '.join(columns)}
                    FROM {etl_table}
                """
        query += f"""{condition_query}
                """

        print("==================[ORACLE QEURY]==================")
        print(query)
        df = pd.read_sql(query, etl_connection)
        print(f"## {etl_table} 조회 카운트 : {len(df)}")

        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            df[col] = df[col].apply(
                lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
            )

        # NaN 값을 명확하게 None으로 변환
        df.replace({np.nan: None}, inplace=True)

        print(df)

        with load_connection.cursor() as load_cursor:

            # MERGE 쿼리 생성 (배치 처리)
            values = df.where(pd.notnull(df), None).values.tolist()

            batch_size = 10000  # 한 번에 실행할 최대 행 수

            for i in range(0, len(values), batch_size):
                temp_table = f"{load_table_name}{chunk_index}"

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

                batch = values[i: i + batch_size]
                load_cursor.executemany(insert_query, batch)

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
                print(f"{temp_table} 머지 완료!!!!")

                chunk_index += 1

    load_connection.close()
    print("커넥션 종료")

CONDITION_QEURY = f"""
WHERE CHG_DTM >= TO_DATE('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS') 
AND CHG_DTM <= TO_DATE('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS') + 1
"""

CONDITION_QEURY2 = f"""
WHERE CHG_DTM >= TO_DATE('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS')
AND CHG_DTM <= TO_DATE('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS')
"""

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_HES",  # DAG의 고유 식별자
        schedule_interval='40 0 * * *',
        start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"),
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑","HES", "ODS"]  # DAG에 붙일 태그
) as dag:
    task_HES_CTPF_BSIC_VAL_DTL = PythonOperator(
        task_id="task_HES_CTPF_BSIC_VAL_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snow_load", var_dict['HES_CTPF_BSIC_VAL_DTL']['ETL_TABLE'],
                 var_dict['HES_CTPF_BSIC_VAL_DTL']['LOAD_TABLE'],
                 var_dict['HES_CTPF_BSIC_VAL_DTL']['COLUMNS'], var_dict['HES_CTPF_BSIC_VAL_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )

    task_HES_CTPF_BSIC_HMALL_DTL = PythonOperator(
        task_id="task_HES_CTPF_BSIC_HMALL_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snow_load", var_dict['HES_CTPF_BSIC_HMALL_DTL']['ETL_TABLE'],
                 var_dict['HES_CTPF_BSIC_HMALL_DTL']['LOAD_TABLE'],
                 var_dict['HES_CTPF_BSIC_HMALL_DTL']['COLUMNS'], var_dict['HES_CTPF_BSIC_HMALL_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )

    task_HES_INSU_ARLT_DTL = PythonOperator(
        task_id="task_HES_INSU_ARLT_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snow_load", var_dict['HES_INSU_ARLT_DTL']['ETL_TABLE'],
                 var_dict['HES_INSU_ARLT_DTL']['LOAD_TABLE'],
                 var_dict['HES_INSU_ARLT_DTL']['COLUMNS'], var_dict['HES_INSU_ARLT_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY2]
    )

    task_HES_GA_INSU_ARLT_DTL = PythonOperator(
        task_id="task_HES_GA_INSU_ARLT_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snow_load", var_dict['HES_GA_INSU_ARLT_DTL']['ETL_TABLE'],
                 var_dict['HES_GA_INSU_ARLT_DTL']['LOAD_TABLE'],
                 var_dict['HES_GA_INSU_ARLT_DTL']['COLUMNS'], var_dict['HES_GA_INSU_ARLT_DTL']['PK_COLUMNS'],
                 ""]
    )

    task_HES_BRND_CTPF_RATE_DTL = PythonOperator(
        task_id="task_HES_BRND_CTPF_RATE_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['HES_BRND_CTPF_RATE_DTL']['ETL_TABLE'], var_dict['HES_BRND_CTPF_RATE_DTL']['LOAD_TABLE'],
                 var_dict['HES_BRND_CTPF_RATE_DTL']['COLUMNS'], var_dict['HES_BRND_CTPF_RATE_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )

    task_HES_BRND_CTPF_RATE_ETC_DTL = PythonOperator(
        task_id="task_HES_BRND_CTPF_RATE_ETC_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['HES_BRND_CTPF_RATE_ETC_DTL']['ETL_TABLE'],
                 var_dict['HES_BRND_CTPF_RATE_ETC_DTL']['LOAD_TABLE'],
                 var_dict['HES_BRND_CTPF_RATE_ETC_DTL']['COLUMNS'], var_dict['HES_BRND_CTPF_RATE_ETC_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )

    task_HES_DRCT_PRCH_LOSS_DTL = PythonOperator(
        task_id="task_HES_DRCT_PRCH_LOSS_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['HES_DRCT_PRCH_LOSS_DTL']['ETL_TABLE'],
                 var_dict['HES_DRCT_PRCH_LOSS_DTL']['LOAD_TABLE'],
                 var_dict['HES_DRCT_PRCH_LOSS_DTL']['COLUMNS'], var_dict['HES_DRCT_PRCH_LOSS_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY2]
    )

    task_RAR_CTPF_RATE_DTL = PythonOperator(
        task_id="task_RAR_CTPF_RATE_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['RAR_CTPF_RATE_DTL']['ETL_TABLE'],
                 var_dict['RAR_CTPF_RATE_DTL']['LOAD_TABLE'],
                 var_dict['RAR_CTPF_RATE_DTL']['COLUMNS'], var_dict['RAR_CTPF_RATE_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )

    task_RAR_CTPF_RATE_ETC_DTL = PythonOperator(
        task_id="task_RAR_CTPF_RATE_ETC_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['RAR_CTPF_RATE_ETC_DTL']['ETL_TABLE'],
                 var_dict['RAR_CTPF_RATE_ETC_DTL']['LOAD_TABLE'],
                 var_dict['RAR_CTPF_RATE_ETC_DTL']['COLUMNS'], var_dict['RAR_CTPF_RATE_ETC_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY2]
    )

    task_RAR_CTPF_RATE_HMALL_DTL = PythonOperator(
        task_id="task_RAR_CTPF_RATE_HMALL_DTL",
        python_callable=ora_to_snow_merge,
        op_args=["conn_oracle_main", "conn_snowflake_etl", var_dict['RAR_CTPF_RATE_HMALL_DTL']['ETL_TABLE'],
                 var_dict['RAR_CTPF_RATE_HMALL_DTL']['LOAD_TABLE'],
                 var_dict['RAR_CTPF_RATE_HMALL_DTL']['COLUMNS'], var_dict['RAR_CTPF_RATE_HMALL_DTL']['PK_COLUMNS'],
                 CONDITION_QEURY]
    )


    [task_HES_CTPF_BSIC_VAL_DTL, task_RAR_CTPF_RATE_DTL]

    task_HES_CTPF_BSIC_VAL_DTL >> task_HES_CTPF_BSIC_HMALL_DTL >>\
    task_HES_INSU_ARLT_DTL >> task_HES_GA_INSU_ARLT_DTL >> \
    task_HES_BRND_CTPF_RATE_DTL >> task_HES_BRND_CTPF_RATE_ETC_DTL >> \
    task_HES_DRCT_PRCH_LOSS_DTL

    task_RAR_CTPF_RATE_DTL >> task_RAR_CTPF_RATE_ETC_DTL >> task_RAR_CTPF_RATE_HMALL_DTL

