from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from common import reverse_task_variables as var
import json
import numpy as np
import pandas as pd
from airflow.models import Variable
import pendulum

var_text = Variable.get('VAR_dag_CDC_ODS_DAILY_TO_HDHS')

var_dict = json.loads(var_text)

def snow_to_snow_merge(etl_conn_id, load_conn_id, etl_table, load_table, columns, pk_columns, condition_query):
    import os
    """
    특정 테이블 데이터를 처리하고 Snowflake에 MERGE 합니다.
    """

    etl_schema, etl_table_name = etl_table.split('.')
    load_schema, load_table_name = load_table.split('.')


    etl_hook = SnowflakeHook(snowflake_conn_id=etl_conn_id)
    load_hook = SnowflakeHook(snowflake_conn_id=load_conn_id)
    load_connection = load_hook.get_conn()

    chunk_index = 1

    with etl_hook.get_conn() as etl_connection:

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


etl_conn_id = 'conn_snowflake_etl'
load_conn_id = 'conn_snow_load'

task_BCU_CUST_TNDC_INF_TO_HDHS_QEURY = f"""WHERE SMR_DT BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD')
"""

task_CU_CUST_MKTG_MST_TO_HDHS_QEURY = f"""WHERE MKTG_RFS_TRGT_YN = 'Y' 
AND NOTC_PRRG_DT BETWEEN TO_CHAR(TO_DATE('{var.daily_main_p_start}' , 'YYYYMMDD') + 1 , 'YYYYMMDD') 
AND TO_CHAR(TO_DATE('{var.daily_main_p_end}' , 'YYYYMMDD') + 1 , 'YYYYMMDD')
"""

task_CU_CUST_STAT_DTL_TO_HDHS_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
OR VLID_TERM_EXPY_PRRG_DT 
BETWEEN TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD')
AND TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD')
"""

task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS_QEURY = f"""WHERE NOTC_PRRG_DT BETWEEN DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_start}', 'YYYYMMDD'))
AND DATEADD(DAY, 1, TO_DATE('{var.daily_main_p_end}', 'YYYYMMDD'))
AND ETL_DTM BETWEEN TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 0, 0, 0) 
AND TIMESTAMP_NTZ_FROM_PARTS(YEAR(CURRENT_DATE), MONTH(CURRENT_DATE), DAY(CURRENT_DATE), 23, 59, 59)
"""

task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_QUERY = f"""WHERE ETL_DTM BETWEEN DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_start}' || '000000', 'YYYYMMDDHH24MISS'))
AND DATEADD(DAY, 1, TO_TIMESTAMP('{var.daily_main_p_end}' || '235959', 'YYYYMMDDHH24MISS'))
"""

REAL_SWERT_QEURY = f"""WHERE APLY_DT >= TO_CHAR(DATEADD(DAY, 1,TO_DATE('{var.daily_main_p_start}','YYYYMMDD')),'YYYYMMDD') 
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


# Define the DAG
with DAG(
    dag_id="dag_CDC_ODS_DAILY_TO_HDHS_v2",
    schedule_interval=None,
    tags=["현대홈쇼핑","ODS","역방향"]
) as dag:

    task_RAR_REAL_SWRT_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_REAL_SWRT_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWERT_QEURY]
    )

    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWERT_QEURY]
    )

    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWERT_QEURY]
    )

    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS = PythonOperator(
        task_id="task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['COLUMNS'], var_dict['task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS']['PK_COLUMNS'],
                 REAL_SWERT_QEURY]
    )

    task_CU_CUST_MKTG_MST_TO_HDHS = PythonOperator(
        task_id="task_CU_CUST_MKTG_MST_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['ETL_TABLE'], var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['COLUMNS'], var_dict['task_CU_CUST_MKTG_AGR_MST_TO_HDHS']['PK_COLUMNS'],
                 task_CU_CUST_MKTG_MST_TO_HDHS_QEURY]
    )

    task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS = PythonOperator(
        task_id="task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['COLUMNS'], var_dict['task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS']['PK_COLUMNS'],
                 task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS_QEURY]
    )

    task_BCU_CUST_TNDC_INF_TO_HDHS = PythonOperator(
        task_id="task_BCU_CUST_TNDC_INF_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['ETL_TABLE'], var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['COLUMNS'], var_dict['task_BCU_CUST_TNDC_INF_TO_HDHS']['PK_COLUMNS'],
                 task_BCU_CUST_TNDC_INF_TO_HDHS_QEURY]
    )

    # task_HES_RNTL_ARLT_DTL = PythonOperator(
    #     task_id="task_HES_RNTL_ARLT_DTL",
    #     python_callable=snow_to_snow_merge,
    #     op_args=[etl_conn_id, load_conn_id, var.HES_RNTL_ARLT_DTL_ETL_TABLE, var.HES_RNTL_ARLT_DTL_LOAD_TABLE,
    #              var.HES_RNTL_ARLT_DTL_COLUMNS, ['SELL_MDA_GBCD', 'SLITM_CD', 'SMR_DT'],
    #              HES_RNTL_ARLT_DTL_QUERY]
    # )

    task_CU_CUST_STAT_DTL_TO_HDHS = PythonOperator(
        task_id="task_CU_CUST_STAT_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['COLUMNS'], var_dict['task_CU_CUST_STAT_DTL_TO_HDHS']['PK_COLUMNS'],
                 task_CU_CUST_STAT_DTL_TO_HDHS_QUERY]
    )

    task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS = PythonOperator(
        task_id="task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['COLUMNS'], var_dict['task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS']['PK_COLUMNS'],
                 task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS_QUERY]
    )

    task_BOD_ORD_DTL_TO_HDHS = PythonOperator(
        task_id="task_BOD_ORD_DTL_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_BOD_ORD_DTL_TO_HDHS']['ETL_TABLE'], var_dict['task_BOD_ORD_DTL_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_BOD_ORD_DTL_TO_HDHS']['COLUMNS'], var_dict['task_BOD_ORD_DTL_TO_HDHS']['PK_COLUMNS'],
                 ETL_DTM_QUERY]
    )

    task_RDM_SELL_MDA_DIM_TO_HDHS = PythonOperator(
        task_id="task_RDM_SELL_MDA_DIM",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['ETL_TABLE'], var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['COLUMNS'], var_dict['task_RDM_SELL_MDA_DIM_TO_HDHS']['PK_COLUMNS'],
                 ETL_DTM_QUERY]
    )


    task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS = PythonOperator(
        task_id="task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS",
        python_callable=snow_to_snow_merge,
        op_args=[etl_conn_id, load_conn_id, var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['ETL_TABLE'], var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['LOAD_TABLE'],
                 var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['COLUMNS'], var_dict['task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS']['PK_COLUMNS'],
                 ETL_DTM_QUERY]
    )

    task_RAR_REAL_SWRT_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ETC_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_DTL_TO_HDHS >> \
    task_RAR_REAL_SWRT_ONLN_ETC_DTL_TO_HDHS >> \
    task_CU_CUST_MKTG_MST_TO_HDHS >> \
    task_CU_MKTG_AGR_EMAIL_DTL_TO_HDHS >> \
    task_BCU_CUST_TNDC_INF_TO_HDHS >> \
    task_CU_CUST_STAT_DTL_TO_HDHS >> \
    task_CU_VLID_TERM_EMAIL_DTL_TO_HDHS >> \
    task_BOD_ORD_DTL_TO_HDHS >> \
    task_RDM_SELL_MDA_DIM_TO_HDHS >> \
    task_RCA_MDA_AREA_CALL_HOU_FCT_TO_HDHS
    # task_HES_RNTL_ARLT_DTL



