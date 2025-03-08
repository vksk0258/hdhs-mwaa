from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.providers.oracle.hooks.oracle import OracleHook
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import datetime
import pendulum

var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')
client_path = Variable.get("client_path")

var_dict = json.loads(var_text)

def snow_to_ora_merge(etl_conn_id, load_conn_id, etl_table, load_table, columns):
    import os
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """


    etl_hook = SnowflakeHook(snowflake_conn_id=etl_conn_id)
    oracle_hook = OracleHook(oracle_conn_id=load_conn_id, thick_mode=True, thick_mode_lib_dir=client_path)
    load_connection = oracle_hook.get_conn()

    chunk_index = 1
    batch_size = 100000

    with load_connection.cursor() as load_cursor:

        truncate_query = f"TRUNCATE TABLE {load_table}"

        print("==================[truncate_query]==================")
        print(truncate_query)

        load_cursor.execute(truncate_query)

    while True:

        offset = (chunk_index - 1) * batch_size

        with etl_hook.get_conn() as etl_connection:

            query = f"""SELECT {', '.join(columns)}
                        FROM {etl_table}
                        ORDER BY ORD_NO, ORD_PTC_SEQ
                        LIMIT {batch_size} OFFSET {offset}
                     """

            print("==================[ORACLE QEURY]==================")
            print(query)

            df = pd.read_sql(query, etl_connection)

            chunk_index += 1

            if df.empty:
                print("No more data to process for this table.")
                break

            for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df[col] = df[col].apply(
                    lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                    else x.strftime('%Y%m%d') if isinstance(x, pd.Timestamp) else str(x)
                )

            # NaN 값을 명확하게 None으로 변환
            df.replace({np.nan: None}, inplace=True)

            with load_connection.cursor() as load_cursor:

                # MERGE 쿼리 생성 (배치 처리)
                values = df.where(pd.notnull(df), None).values.tolist()

                load_batch_size = 10000  # 한 번에 실행할 최대 행 수

                for i in range(0, len(values), load_batch_size):

                    insert_query = f"""
                        INSERT INTO {load_table} ({", ".join(columns)})
                        VALUES ({", ".join([
                        f"TO_DATE(:{i + 1}, 'YYYY-MM-DD HH24:MI:SS')" if 'DT' in col and col in ['ETL_DTM']
                        else f":{i + 1}" for i, col in enumerate(columns)
                    ])})
                    """

                    batch = values[i: i + load_batch_size]
                    load_cursor.executemany(insert_query, batch)

                    # 커밋 수행
                    load_connection.commit()


        print(f"=========누적 {chunk_index * batch_size - 100000}건 적재 완료============")


    load_connection.close()
    print("초기적재 완료")


etl_conn_id = 'conn_snowflake_etl'
load_conn_id = 'conn_oracle_main'


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_DAILY_TO_HDHS_initial",
    schedule_interval=None,
    dagrun_timeout=datetime.timedelta(minutes=10000),
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:


    task_BOD_ORD_DTL_TO_HDHS = PythonOperator(
        task_id="task_BOD_ORD_DTL_TO_HDHS",
        python_callable=snow_to_ora_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_BOD_ORD_DTL_TO_HDHS']['ETL_TABLE'], "HDHS_DW.BOD_ORD_DTL",
                 ["ORD_NO", "ORD_PTC_SEQ", "ORD_DT", "ORD_SRC_DT", "CUST_NO", "SLITM_CD", "ACPT_CH_GBCD", "SELL_MDA_NO", "ORD_SRC_GBCD", "ORD_ITEM_GBCD", "ORD_QTY", "ETL_DTM"]]
    )


    task_BOD_ORD_DTL_TO_HDHS



