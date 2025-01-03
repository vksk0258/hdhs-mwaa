from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import json
import boto3
import pendulum

informix_jdbc = Variable.get("informix_jdbc")

# Informix 컬럼 정의
dagent_columns = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL", "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME", "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS", "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME", "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4", "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2", "I_OTHERSTBYTIME", "I_AUXSTBYTIME"]
# Snowflake 키 컬럼 정의
primary_keys = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL"]

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_01",
        schedule_interval=None,
        catchup=False,
        tags=["현대홈쇼핑"]
) as dag:

    @task(task_id='load_data_from_informix')
    def load_data_from_informix(**kwargs):
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

        # Informix에서 데이터 조회
        jdbc_hook = JdbcHook(jdbc_conn_id='conn_informix_locus1', driver_path=informix_jdbc,
                             driver_class='com.informix.jdbc.IfxDriver')
        query = f"""
            SELECT {', '.join(dagent_columns)}
            FROM DAGENT
            WHERE ROW_DATE >= DATE('{fm_p_start}') 
              AND ROW_DATE <= DATE('{fm_p_end}')
        """
        with jdbc_hook.get_conn() as conn:
            df = pd.read_sql(query, conn)
        return df

    @task(task_id='update_snowflake_table')
    def update_snowflake_table(df, **kwargs):
        # ETL_DTM 열 추가 (현재 시간)
        df['ETL_DTM'] = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')

        # Snowflake 연결
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

        # Snowflake 테이블 이름
        snowflake_table = "DWCT_DAGENT"
        snowflake_schema = "ODS_CMS"
        snowflake_database = "DW_LOAD_DB"

        # 임시 테이블에 데이터 업로드
        temp_table = f"{snowflake_table}_TEMP"
        with snowflake_hook.get_conn() as conn:
            cursor = conn.cursor()

            # DataFrame을 Snowflake에 업로드
            snowflake_hook.write_pandas(
                df,
                database=snowflake_database,
                schema=snowflake_schema,
                table_name=temp_table,
                quote_identifiers=True,
                overwrite=True,
            )

            # Snowflake Merge Query
            merge_query = f"""
            MERGE INTO {snowflake_database}.{snowflake_schema}.{snowflake_table} AS target
            USING {snowflake_database}.{snowflake_schema}.{temp_table} AS source
            ON {" AND ".join([f"target.{col} = source.{col}" for col in primary_keys])}
            WHEN MATCHED THEN
                UPDATE SET {", ".join([f"target.{col} = source.{col}" for col in dagent_columns])}, target.ETL_DTM = source.ETL_DTM
            WHEN NOT MATCHED THEN
                INSERT ({", ".join(dagent_columns + ['ETL_DTM'])})
                VALUES ({", ".join([f"source.{col}" for col in dagent_columns + ['ETL_DTM']])});
            """
            cursor.execute(merge_query)

            # 임시 테이블 삭제
            cursor.execute(f"DROP TABLE IF EXISTS {snowflake_database}.{snowflake_schema}.{temp_table}")

        print("Data successfully merged into Snowflake.")

        # DAG 실행
    df = load_data_from_informix()
    update_snowflake_table(df)

