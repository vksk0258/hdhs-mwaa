from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from common.notify_error_functions import notify_api_on_error
import pendulum
import numpy as np
import pandas as pd
from airflow.operators.python import PythonOperator
import boto3
import datetime
from airflow.models import Variable
import json
import os

# S3 JSON 파라미터 로드
s3 = boto3.client('s3')
bucket_name = "hdhs-dw-mwaa-s3"
key = "param/wf_DD01_0400_CMS_01.json"
response = s3.get_object(Bucket=bucket_name, Key=key)
params = json.load(response['Body'])

p_start = params.get("$$P_START")
p_end = params.get("$$P_END")

fm_p_start = f"{p_start[:4]}-{p_start[4:6]}-{p_start[6:]}"
fm_p_end = f"{p_end[:4]}-{p_end[4:6]}-{p_end[6:]}"

informix_jdbc = Variable.get("informix_jdbc")
informix_jdbc_jc = Variable.get("informix_jdbc_jc")

snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snowflake_load_ods')

informixdb_hook = JdbcHook(jdbc_conn_id="conn_informix_locus1", driver_path=informix_jdbc,
                           driver_class='com.informix.jdbc.IfxDriver')

dagent_columns = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL", "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME", "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS", "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME", "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4", "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2", "I_OTHERSTBYTIME", "I_AUXSTBYTIME"]
# Snowflake 키 컬럼 정의
dagent_primary_keys = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL"]
hagent_primary_keys = ["ROW_DATE","STARTTIME", "INTRVL", "ACD", "SPLIT", "EXTENSION", "LOGID", "RSV_LEVEL"]

hagent_columns = [
    "ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL",
    "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME",
    "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME",
    "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS",
    "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2",
    "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME",
    "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME",
    "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME",
    "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS",
    "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME",
    "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS",
    "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME",
    "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4",
    "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2",
    "I_OTHERSTBYTIME", "I_AUXSTBYTIME"
]

dsplit_columns = [
    "ROW_DATE", "ACD", "SPLIT", "I_STAFFTIME", "I_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME",
    "I_ACWINTIME", "I_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "MAXSTAFFED", "ACWINCALLS",
    "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME",
    "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS",
    "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS",
    "INFLOWCALLS", "ACDCALLS", "ANSTIME", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME",
    "ACDCALLS1", "ACDCALLS2", "ACDCALLS3", "ACDCALLS4", "ACDCALLS5", "ACDCALLS6", "ACDCALLS7", "ACDCALLS8",
    "ACDCALLS9", "ACDCALLS10", "BACKUPCALLS", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED",
    "CONFERENCE", "ABNCALLS", "ABNTIME", "ABNCALLS1", "ABNCALLS2", "ABNCALLS3", "ABNCALLS4", "ABNCALLS5",
    "ABNCALLS6", "ABNCALLS7", "ABNCALLS8", "ABNCALLS9", "ABNCALLS10", "DEQUECALLS", "DEQUETIME", "BUSYCALLS",
    "BUSYTIME", "DISCCALLS", "DISCTIME", "OUTFLOWCALLS", "OUTFLOWTIME", "INTERFLOWCALLS", "LOWCALLS",
    "MEDCALLS", "HIGHCALLS", "TOPCALLS", "ACCEPTABLE", "SERVICELEVEL", "PERIOD1", "PERIOD2", "PERIOD3",
    "PERIOD4", "PERIOD5", "PERIOD6", "PERIOD7", "PERIOD8", "PERIOD9", "MAXINQUEUE", "MAXOCWTIME",
    "CALLSOFFERED", "PERIODCHG", "SVCLEVELCHG", "I_RINGTIME", "RINGTIME", "RINGCALLS", "ABNRINGCALLS",
    "O_ABNCALLS", "O_OTHERCALLS", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME",
    "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME",
    "PHANTOMABNS", "OTHERCALLS", "OTHERTIME", "SLVLABNS", "SLVLOUTFLOWS", "I_ARRIVED", "I_AUXTIME0",
    "I_AUXTIME1", "I_AUXTIME2", "I_AUXTIME3", "I_AUXTIME4", "I_AUXTIME5", "I_AUXTIME6", "I_AUXTIME7",
    "I_AUXTIME8", "I_AUXTIME9", "I_DA_ACDTIME", "I_DA_ACWTIME", "I_TAVAILTIME", "I_TAUXTIME", "MAXTOP",
    "I_NORMTIME", "I_OL1TIME", "I_OL2TIME", "I_TOTHERTIME", "MAX_TOT_PERCENTS", "ACDCALLS_R1", "ACDCALLS_R2",
    "I_ACDTIME_R1", "I_ACDTIME_R2", "I_ACWTIME_R1", "I_ACWTIME_R2", "I_RINGTIME_R1", "I_RINGTIME_R2",
    "I_OTHERTIME_R1", "I_OTHERTIME_R2", "I_AUXTIME_R1", "I_AUXTIME_R2", "I_OTHERSTBYTIME_R1",
    "I_OTHERSTBYTIME_R2", "I_AUXSTBYTIME_R1", "I_AUXSTBYTIME_R2", "I_BEHINDTIME", "I_AUTORESERVETIME",
    "TARGETPERCENT", "TARGETPCTCHG", "TARGETSECONDS", "TARGETSECCHG", "TARGETACDCALLS", "TARGETABNS",
    "TARGETOUTFLOWS"
]
hsplit_columns = [
    "ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "I_STAFFTIME", "I_AVAILTIME", "I_ACDTIME", "I_ACWTIME",
    "I_ACWOUTTIME", "I_ACWINTIME", "I_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "MAXSTAFFED",
    "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS",
    "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME",
    "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9",
    "ASSISTS", "INFLOWCALLS", "ACDCALLS", "ANSTIME", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME",
    "ACDCALLS1", "ACDCALLS2", "ACDCALLS3", "ACDCALLS4", "ACDCALLS5", "ACDCALLS6", "ACDCALLS7", "ACDCALLS8",
    "ACDCALLS9", "ACDCALLS10", "BACKUPCALLS", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED",
    "CONFERENCE", "ABNCALLS", "ABNTIME", "ABNCALLS1", "ABNCALLS2", "ABNCALLS3", "ABNCALLS4", "ABNCALLS5",
    "ABNCALLS6", "ABNCALLS7", "ABNCALLS8", "ABNCALLS9", "ABNCALLS10", "DEQUECALLS", "DEQUETIME", "BUSYCALLS",
    "BUSYTIME", "DISCCALLS", "DISCTIME", "OUTFLOWCALLS", "OUTFLOWTIME", "INTERFLOWCALLS", "LOWCALLS", "MEDCALLS",
    "HIGHCALLS", "TOPCALLS", "ACCEPTABLE", "SERVICELEVEL", "PERIOD1", "PERIOD2", "PERIOD3", "PERIOD4", "PERIOD5",
    "PERIOD6", "PERIOD7", "PERIOD8", "PERIOD9", "MAXINQUEUE", "MAXOCWTIME", "CALLSOFFERED", "PERIODCHG",
    "SVCLEVELCHG", "I_RINGTIME", "RINGTIME", "RINGCALLS", "ABNRINGCALLS", "O_ABNCALLS", "O_OTHERCALLS",
    "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS",
    "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "OTHERCALLS", "OTHERTIME",
    "SLVLABNS", "SLVLOUTFLOWS", "I_ARRIVED", "I_AUXTIME0", "I_AUXTIME1", "I_AUXTIME2", "I_AUXTIME3",
    "I_AUXTIME4", "I_AUXTIME5", "I_AUXTIME6", "I_AUXTIME7", "I_AUXTIME8", "I_AUXTIME9", "I_DA_ACDTIME",
    "I_DA_ACWTIME", "I_TAVAILTIME", "I_TAUXTIME", "MAXTOP", "I_NORMTIME", "I_OL1TIME", "I_OL2TIME", "I_TOTHERTIME",
    "MAX_TOT_PERCENTS", "ACDCALLS_R1", "ACDCALLS_R2", "I_ACDTIME_R1", "I_ACDTIME_R2", "I_ACWTIME_R1",
    "I_ACWTIME_R2", "I_RINGTIME_R1", "I_RINGTIME_R2", "I_OTHERTIME_R1", "I_OTHERTIME_R2", "I_AUXTIME_R1",
    "I_AUXTIME_R2", "I_OTHERSTBYTIME_R1", "I_OTHERSTBYTIME_R2", "I_AUXSTBYTIME_R1", "I_AUXSTBYTIME_R2",
    "I_BEHINDTIME", "I_AUTORESERVETIME", "TARGETPERCENT", "TARGETPCTCHG", "TARGETSECONDS", "TARGETSECCHG",
    "TARGETACDCALLS", "TARGETABNS", "TARGETOUTFLOWS"
]

hsplit_primary_keys = ["ROW_DATE", "STARTTIME", "INTRVL", "ACD", "SPLIT", "I_ACWINTIME"]
dsplit_primary_keys = ["ROW_DATE", "ACD", "SPLIT"]

dagent_condition_query = f"""
                 WHERE ROW_DATE IS NOT NULL
                 AND ACD IS NOT NULL
                 AND SPLIT IS NOT NULL
                 AND EXTENSION IS NOT NULL
                 AND LOGID IS NOT NULL
                 AND LOC_ID IS NOT NULL
                 AND RSV_LEVEL IS NOT NULL
                 AND row_date >= TO_DATE('{p_start}','%Y%m%d')
                 AND row_date <= TO_DATE('{p_end}','%Y%m%d')
"""

hagent_condition_query = f"""
                 WHERE ROW_DATE IS NOT NULL
                 AND STARTTIME IS NOT NULL
                 AND INTRVL IS NOT NULL
                 AND ACD IS NOT NULL
                 AND SPLIT IS NOT NULL
                 AND EXTENSION IS NOT NULL
                 AND LOGID IS NOT NULL
                 AND LOC_ID IS NOT NULL
                 AND RSV_LEVEL IS NOT NULL
                 AND row_date >= TO_DATE('{p_start}','%Y%m%d')
                 AND row_date <= TO_DATE('{p_end}','%Y%m%d')
            """

def delete_existing_files(bucket_name, prefix):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    if "Contents" in response:
        for obj in response["Contents"]:
            s3.delete_object(Bucket=bucket_name, Key=obj["Key"])
        print(f"Deleted existing files in S3 path: s3://{bucket_name}/{prefix}")
    else:
        print(f"No existing files to delete in S3 path: s3://{bucket_name}/{prefix}")

def task_DWCT_HSPLIT_c_01 (table, hsplit_columns, hsplit_primary_keys):
    schema, table_name = table.split('.')

    columns = hsplit_columns

    # Constants
    s3_bucket_name = "hdhs-dw-migdata-s3"
    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/informix_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)
    informixdb_connection = informixdb_hook.get_conn()

    # 각 컬럼에 대해 NOT NULL 조건을 생성
    not_null_conditions = f"ROW_DATE >= DATE('{fm_p_start}') AND ROW_DATE <= DATE('{fm_p_end}') AND "
    not_null_conditions += " AND ".join([f"{key} IS NOT NULL" for key in hsplit_primary_keys])

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE {not_null_conditions}
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    if df.empty:
        print("No more data to process for this table.")
        return None

    print(f"## {table} 조회 카운트 : {len(df)}")

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    # NaN 값을 명확하게 None으로 변환
    df.replace({np.nan: None}, inplace=True)

    values = df.where(pd.notnull(df), None).values.tolist()

    batch_size = 50000  # 한 번에 실행할 최대 행 수

    chunk_index = 1

    for i in range(0, len(values), batch_size):
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:
            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(
                f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in hsplit_primary_keys])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in hsplit_primary_keys])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            merge_query = f"""
                            MERGE INTO {schema}.{table_name} AS target
                            USING {schema}.{temp_table_name} AS source
                            ON {merge_condition}
                            WHEN MATCHED THEN
                                UPDATE SET {update_set}
                            WHEN NOT MATCHED THEN
                                INSERT ({insert_columns})
                                VALUES ({insert_values});
                            """

            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            print(f"{temp_table_name} 머지 완료!!!!")

def task_DWCT_DSPLIT_c_01 (table, dsplit_columns, dsplit_primary_keys):
    schema, table_name = table.split('.')

    columns = dsplit_columns

    # Constants
    s3_bucket_name = "hdhs-dw-migdata-s3"
    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/informix_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)
    informixdb_connection = informixdb_hook.get_conn()

    # 각 컬럼에 대해 NOT NULL 조건을 생성
    not_null_conditions = f"ROW_DATE >= DATE('{fm_p_start}') AND ROW_DATE <= DATE('{fm_p_end}') AND "
    not_null_conditions += f"""(ROW_DATE IS NOT NULL AND ACD IS NOT NULL AND SPLIT IS NOT NULL AND ROW_DATE <> '050108')
            OR (ROW_DATE = '050108' AND ACD = 1)
            OR (ROW_DATE = '050108' AND ACD = 2 AND SPLIT <> 900)"""

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE {not_null_conditions}
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    if df.empty:
        print("No more data to process for this table.")
        return None

    print(f"## {table} 조회 카운트 : {len(df)}")

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    # NaN 값을 명확하게 None으로 변환
    df.replace({np.nan: None}, inplace=True)

    values = df.where(pd.notnull(df), None).values.tolist()

    batch_size = 50000  # 한 번에 실행할 최대 행 수

    chunk_index = 1

    for i in range(0, len(values), batch_size):
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:
            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(
                f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in dsplit_primary_keys])

            # UPDATE SET 구문 생성 (각 컬럼에 대해 target 컬럼 = source 컬럼 값을 지정)
            update_set = ", ".join([f"target.{col} = source.{col}" for col in columns if col not in dsplit_primary_keys])

            # INSERT 구문 생성
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            # 최종 MERGE 쿼리 생성
            merge_query = f"""
                    MERGE INTO {schema}.{table_name} AS target
                    USING {schema}.{temp_table_name} AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_columns})
                        VALUES ({insert_values});
                """
            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            print(f"{temp_table_name} 머지 완료!!!!")

def task_DWCT_DAGENT_c_01 (table, dagent_columns, dagent_primary_keys):
    schema, table_name = table.split('.')

    columns = dagent_columns

    # Constants
    s3_bucket_name = "hdhs-dw-migdata-s3"
    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/informix_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)
    informixdb_connection = informixdb_hook.get_conn()

    # 각 컬럼에 대해 NOT NULL 조건을 생성
    not_null_conditions = f"ROW_DATE >= DATE('{fm_p_start}') AND ROW_DATE <= DATE('{fm_p_end}') AND "
    not_null_conditions += " AND ".join([f"{key} IS NOT NULL" for key in dagent_primary_keys])

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE {not_null_conditions}
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    if df.empty:
        print("No more data to process for this table.")
        return None

    print(f"## {table} 조회 카운트 : {len(df)}")

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    # NaN 값을 명확하게 None으로 변환
    df.replace({np.nan: None}, inplace=True)

    # LOGID 컬럼에서 공백 제거
    df['logid'] = df['logid'].str.replace(' ', '')

    values = df.where(pd.notnull(df), None).values.tolist()

    batch_size = 50000  # 한 번에 실행할 최대 행 수

    chunk_index = 1

    for i in range(0, len(values), batch_size):
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:
            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(
                f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in dagent_primary_keys])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in dagent_primary_keys])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            merge_query = f"""
                        MERGE INTO {schema}.{table_name} AS target
                        USING {schema}.{temp_table_name} AS source
                        ON {merge_condition}
                        WHEN MATCHED THEN
                            UPDATE SET {update_set}
                        WHEN NOT MATCHED THEN
                            INSERT ({insert_columns})
                            VALUES ({insert_values});
                        """

            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            print(f"{temp_table_name} 머지 완료!!!!")


def task_DWCT_HAGENT_c_01 (table, hagent_columns, hagent_primary_keys):
    schema, table_name = table.split('.')

    columns = hagent_columns

    # Constants
    s3_bucket_name = "hdhs-dw-migdata-s3"
    temp_table_name = table_name+"_TEMP"

    tmp_dir = f"/tmp/informix_{temp_table_name}"
    s3_prefix = f"dw/dms_full_load/{schema}/{temp_table_name}/"

    os.makedirs(tmp_dir, exist_ok=True)

    # 기존 파일 삭제
    delete_existing_files(s3_bucket_name, s3_prefix)
    informixdb_connection = informixdb_hook.get_conn()

    # 각 컬럼에 대해 NOT NULL 조건을 생성
    not_null_conditions = f"ROW_DATE >= DATE('{fm_p_start}') AND ROW_DATE <= DATE('{fm_p_end}') AND "
    not_null_conditions += " AND ".join([f"{key} IS NOT NULL" for key in hagent_primary_keys])

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE {not_null_conditions}
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    if df.empty:
        print("No more data to process for this table.")
        return None

    print(f"## {table} 조회 카운트 : {len(df)}")

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    # NaN 값을 명확하게 None으로 변환
    df.replace({np.nan: None}, inplace=True)

    values = df.where(pd.notnull(df), None).values.tolist()

    batch_size = 50000  # 한 번에 실행할 최대 행 수

    chunk_index = 1

    for i in range(0, len(values), batch_size):
        s3_key = f"{s3_prefix}LOAD{chunk_index:08d}.parquet"
        file_name = f"{tmp_dir}/LOAD{chunk_index:08d}.parquet"

        df.to_parquet(file_name, engine='pyarrow', index=False)
        print(f"Data batch {chunk_index} saved to {file_name}")

        os.system(f"aws s3 cp {file_name} s3://{s3_bucket_name}/{s3_key}")
        print(f"Uploaded {file_name} to s3://{s3_bucket_name}/{s3_key}")

        os.remove(file_name)
        print(f"Deleted {file_name} from local directory")

        chunk_index += 1

    with snowflake_hook.get_conn() as snowflake_conn:
        with snowflake_conn.cursor() as snowflake_cursor:
            create_temp_table_query = f"""
                            CREATE OR REPLACE TABLE {schema}.{temp_table_name} AS
                            SELECT * FROM {schema}.{table_name} WHERE 1=0;
                            """  # 빈 임시 테이블 생성

            print("==================[create_temp_table_query]==================")
            print(create_temp_table_query)

            snowflake_cursor.execute(create_temp_table_query)

            print("임시 테이블 생성 성공!")

            snowflake_cursor.execute(
                f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")
            print(f"CALL DW_LOAD_DB.CONFIG.PROC_INIT_COPY('DW_LOAD_DB', '{schema}', '{temp_table_name}')")

            print("임시 테이블 데이터 로드 성공!")

            # 3️⃣ MERGE 실행
            merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in hagent_primary_keys])

            update_set = ", ".join(
                [f"target.{col} = source.{col}" for col in columns if col not in hagent_primary_keys])
            insert_columns = ", ".join(columns)
            insert_values = ", ".join([f"source.{col}" for col in columns])

            merge_query = f"""
                                        MERGE INTO {schema}.{table_name} AS target
                                        USING {schema}.{temp_table_name} AS source
                                        ON {merge_condition}
                                        WHEN MATCHED THEN
                                            UPDATE SET {update_set}
                                        WHEN NOT MATCHED THEN
                                            INSERT ({insert_columns})
                                            VALUES ({insert_values})
                                        """

            print("==================[merge_query]==================")
            print(merge_query)

            snowflake_cursor.execute(merge_query)
            print(f"{temp_table_name} 머지 완료!!!!")


# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_01",
        schedule='30 4 * * *',
        start_date=pendulum.datetime(2025, 3, 19, tz="Asia/Seoul"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=60),
        tags=["현대홈쇼핑","DD01_0030_DAILY_MAIN"]
) as dag:


    task_DWCT_HSPLIT_c_01 = PythonOperator(
        task_id = "task_DWCT_HSPLIT_c_01",
        python_callable=task_DWCT_HSPLIT_c_01,
        op_args=['ODS_CMS.DWCT_HSPLIT', hsplit_columns, hsplit_primary_keys],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_DWCT_DSPLIT_c_01 = PythonOperator(
        task_id = "task_DWCT_DSPLIT_c_01",
        python_callable=task_DWCT_DSPLIT_c_01,
        op_args=['ODS_CMS.DWCT_DSPLIT',dsplit_columns, dsplit_primary_keys],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )

    task_DWCT_DAGENT_c_01 = PythonOperator(
        task_id = "task_DWCT_DAGENT_c_01",
        python_callable=task_DWCT_DAGENT_c_01,
        op_args=['ODS_CMS.DWCT_DAGENT', dagent_columns, dagent_primary_keys],
        trigger_rule="all_done",
        provide_context=True,
        on_failure_callback=notify_api_on_error
    )


    task_DWCT_HSPLIT_c_01 >> task_DWCT_DSPLIT_c_01 >> task_DWCT_DAGENT_c_01

