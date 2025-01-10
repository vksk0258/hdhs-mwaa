from airflow import DAG
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
import json
import boto3
import pendulum
import datetime
import os

informix_jdbc = Variable.get("informix_jdbc")

# Informix 컬럼 정의
dagent_columns = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL", "I_STAFFTIME", "TI_STAFFTIME", "I_AVAILTIME", "TI_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "TI_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "ACDCALLS", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "DA_ACDCALLS", "DA_ANSTIME", "DA_ABNCALLS", "DA_ABNTIME", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "I_RINGTIME", "I_DA_ACDTIME", "I_DA_ACWTIME", "DA_ACDTIME", "DA_ACWTIME", "DA_OTHERCALLS", "DA_OTHERTIME", "RINGCALLS", "RINGTIME", "ANSRINGTIME", "TI_OTHERTIME", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "DA_ACWOADJCALLS", "DA_ACWOOFFCALLS", "DA_ACWOOFFTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "I_AUXTIME", "HOLDACDTIME", "DA_RELEASE", "ACD_RELEASE", "TI_AUXTIME0", "TI_AUXTIME1", "TI_AUXTIME2", "TI_AUXTIME3", "TI_AUXTIME4", "TI_AUXTIME5", "TI_AUXTIME6", "TI_AUXTIME7", "TI_AUXTIME8", "TI_AUXTIME9", "ACDCALLS_R1", "ACDCALLS_R2", "I_OTHERSTBYTIME", "I_AUXSTBYTIME"]
# Snowflake 키 컬럼 정의
primary_keys = ["ROW_DATE", "ACD", "SPLIT", "EXTENSION", "LOGID", "LOC_ID", "RSV_LEVEL"]

dsplit_columns = ["ROW_DATE", "ACD", "SPLIT", "I_STAFFTIME", "I_AVAILTIME", "I_ACDTIME", "I_ACWTIME", "I_ACWOUTTIME", "I_ACWINTIME", "I_AUXTIME", "I_AUXOUTTIME", "I_AUXINTIME", "I_OTHERTIME", "MAXSTAFFED", "ACWINCALLS", "ACWINTIME", "AUXINCALLS", "AUXINTIME", "ACWOUTCALLS", "ACWOUTTIME", "ACWOUTOFFCALLS", "ACWOUTOFFTIME", "ACWOUTADJCALLS", "AUXOUTCALLS", "AUXOUTTIME", "AUXOUTOFFCALLS", "AUXOUTOFFTIME", "AUXOUTADJCALLS", "EVENT1", "EVENT2", "EVENT3", "EVENT4", "EVENT5", "EVENT6", "EVENT7", "EVENT8", "EVENT9", "ASSISTS", "INFLOWCALLS", "ACDCALLS", "ANSTIME", "ACDTIME", "ACWTIME", "O_ACDCALLS", "O_ACDTIME", "O_ACWTIME", "ACDCALLS1", "ACDCALLS2", "ACDCALLS3", "ACDCALLS4", "ACDCALLS5", "ACDCALLS6", "ACDCALLS7", "ACDCALLS8", "ACDCALLS9", "ACDCALLS10", "BACKUPCALLS", "HOLDCALLS", "HOLDTIME", "HOLDABNCALLS", "TRANSFERRED", "CONFERENCE", "ABNCALLS", "ABNTIME", "ABNCALLS1", "ABNCALLS2", "ABNCALLS3", "ABNCALLS4", "ABNCALLS5", "ABNCALLS6", "ABNCALLS7", "ABNCALLS8", "ABNCALLS9", "ABNCALLS10", "DEQUECALLS", "DEQUETIME", "BUSYCALLS", "BUSYTIME", "DISCCALLS", "DISCTIME", "OUTFLOWCALLS", "OUTFLOWTIME", "INTERFLOWCALLS", "LOWCALLS", "MEDCALLS", "HIGHCALLS", "TOPCALLS", "ACCEPTABLE", "SERVICELEVEL", "PERIOD1", "PERIOD2", "PERIOD3", "PERIOD4", "PERIOD5", "PERIOD6", "PERIOD7", "PERIOD8", "PERIOD9", "MAXINQUEUE", "MAXOCWTIME", "CALLSOFFERED", "PERIODCHG", "SVCLEVELCHG", "I_RINGTIME", "RINGTIME", "RINGCALLS", "ABNRINGCALLS", "O_ABNCALLS", "O_OTHERCALLS", "DA_ACWINCALLS", "DA_ACWINTIME", "DA_ACWOCALLS", "DA_ACWOTIME", "NOANSREDIR", "INCOMPLETE", "ACDAUXOUTCALLS", "I_ACDAUX_OUTTIME", "I_ACDAUXINTIME", "I_ACDOTHERTIME", "PHANTOMABNS", "OTHERCALLS", "OTHERTIME", "SLVLABNS", "SLVLOUTFLOWS", "I_ARRIVED", "I_AUXTIME0", "I_AUXTIME1", "I_AUXTIME2", "I_AUXTIME3", "I_AUXTIME4", "I_AUXTIME5", "I_AUXTIME6", "I_AUXTIME7", "I_AUXTIME8", "I_AUXTIME9", "I_DA_ACDTIME", "I_DA_ACWTIME", "I_TAVAILTIME", "I_TAUXTIME", "MAXTOP", "I_NORMTIME", "I_OL1TIME", "I_OL2TIME", "I_TOTHERTIME", "MAX_TOT_PERCENTS", "ACDCALLS_R1", "ACDCALLS_R2", "I_ACDTIME_R1", "I_ACDTIME_R2", "I_ACWTIME_R1", "I_ACWTIME_R2", "I_RINGTIME_R1", "I_RINGTIME_R2", "I_OTHERTIME_R1", "I_OTHERTIME_R2", "I_AUXTIME_R1", "I_AUXTIME_R2", "I_OTHERSTBYTIME_R1", "I_OTHERSTBYTIME_R2", "I_AUXSTBYTIME_R1", "I_AUXSTBYTIME_R2", "I_BEHINDTIME", "I_AUTORESERVETIME", "TARGETPERCENT", "TARGETPCTCHG", "TARGETSECONDS", "TARGETSECCHG", "TARGETACDCALLS", "TARGETABNS", "TARGETOUTFLOWS"]


# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_01",
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=2400),
        tags=["현대홈쇼핑"]
) as dag:

    @task(task_id='task_DWCT_DAGENT_c_01')
    def task_DWCT_DAGENT_c_01():
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

    @task(task_id='task_DWCT_DAGENT_c_02')
    def task_DWCT_DAGENT_c_02(df, **kwargs):
        # ETL_DTM 열 추가 (현재 시간)
        df['ETL_DTM'] = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul")).strftime('%Y-%m-%d %H:%M:%S')

        # Snowflake 연결
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

        # Snowflake 테이블 이름
        snowflake_table = "DWCT_DAGENT_TEMP"
        snowflake_schema = "ODS_CMS"

        engine = snowflake_hook.get_sqlalchemy_engine()

        df.to_sql(snowflake_table, con=engine, schema=snowflake_schema, if_exists='append', index=False)

        with snowflake_hook.get_conn() as conn:
            with conn.cursor() as cur:
                merge_query="""
                MERGE INTO DW_LOAD_DB.ODS_CMS.DWCT_DAGENT AS MAIN
                USING DW_LOAD_DB.ODS_CMS.DWCT_DAGENT_TEMP AS TEMP
                ON MAIN.ROW_DATE = TEMP.ROW_DATE
                   AND MAIN.ACD = TEMP.ACD
                   AND MAIN.SPLIT = TEMP.SPLIT
                   AND MAIN.EXTENSION = TEMP.EXTENSION
                   AND MAIN.LOGID = TEMP.LOGID
                   AND MAIN.LOC_ID = TEMP.LOC_ID
                   AND MAIN.RSV_LEVEL = TEMP.RSV_LEVEL
                WHEN MATCHED THEN
                    UPDATE SET
                        MAIN.I_STAFFTIME = TEMP.I_STAFFTIME,
                        MAIN.TI_STAFFTIME = TEMP.TI_STAFFTIME,
                        MAIN.I_AVAILTIME = TEMP.I_AVAILTIME,
                        MAIN.TI_AVAILTIME = TEMP.TI_AVAILTIME,
                        MAIN.I_ACDTIME = TEMP.I_ACDTIME,
                        MAIN.I_ACWTIME = TEMP.I_ACWTIME,
                        MAIN.I_ACWOUTTIME = TEMP.I_ACWOUTTIME,
                        MAIN.I_ACWINTIME = TEMP.I_ACWINTIME,
                        MAIN.TI_AUXTIME = TEMP.TI_AUXTIME,
                        MAIN.I_AUXOUTTIME = TEMP.I_AUXOUTTIME,
                        MAIN.I_AUXINTIME = TEMP.I_AUXINTIME,
                        MAIN.I_OTHERTIME = TEMP.I_OTHERTIME,
                        MAIN.ACWINCALLS = TEMP.ACWINCALLS,
                        MAIN.ACWINTIME = TEMP.ACWINTIME,
                        MAIN.AUXINCALLS = TEMP.AUXINCALLS,
                        MAIN.AUXINTIME = TEMP.AUXINTIME,
                        MAIN.ACWOUTCALLS = TEMP.ACWOUTCALLS,
                        MAIN.ACWOUTTIME = TEMP.ACWOUTTIME,
                        MAIN.ACWOUTOFFCALLS = TEMP.ACWOUTOFFCALLS,
                        MAIN.ACWOUTOFFTIME = TEMP.ACWOUTOFFTIME,
                        MAIN.ACWOUTADJCALLS = TEMP.ACWOUTADJCALLS,
                        MAIN.AUXOUTCALLS = TEMP.AUXOUTCALLS,
                        MAIN.AUXOUTTIME = TEMP.AUXOUTTIME,
                        MAIN.AUXOUTOFFCALLS = TEMP.AUXOUTOFFCALLS,
                        MAIN.AUXOUTOFFTIME = TEMP.AUXOUTOFFTIME,
                        MAIN.AUXOUTADJCALLS = TEMP.AUXOUTADJCALLS,
                        MAIN.EVENT1 = TEMP.EVENT1,
                        MAIN.EVENT2 = TEMP.EVENT2,
                        MAIN.EVENT3 = TEMP.EVENT3,
                        MAIN.EVENT4 = TEMP.EVENT4,
                        MAIN.EVENT5 = TEMP.EVENT5,
                        MAIN.EVENT6 = TEMP.EVENT6,
                        MAIN.EVENT7 = TEMP.EVENT7,
                        MAIN.EVENT8 = TEMP.EVENT8,
                        MAIN.EVENT9 = TEMP.EVENT9,
                        MAIN.ASSISTS = TEMP.ASSISTS,
                        MAIN.ACDCALLS = TEMP.ACDCALLS,
                        MAIN.ACDTIME = TEMP.ACDTIME,
                        MAIN.ACWTIME = TEMP.ACWTIME,
                        MAIN.O_ACDCALLS = TEMP.O_ACDCALLS,
                        MAIN.O_ACDTIME = TEMP.O_ACDTIME,
                        MAIN.O_ACWTIME = TEMP.O_ACWTIME,
                        MAIN.DA_ACDCALLS = TEMP.DA_ACDCALLS,
                        MAIN.DA_ANSTIME = TEMP.DA_ANSTIME,
                        MAIN.DA_ABNCALLS = TEMP.DA_ABNCALLS,
                        MAIN.DA_ABNTIME = TEMP.DA_ABNTIME,
                        MAIN.HOLDCALLS = TEMP.HOLDCALLS,
                        MAIN.HOLDTIME = TEMP.HOLDTIME,
                        MAIN.HOLDABNCALLS = TEMP.HOLDABNCALLS,
                        MAIN.TRANSFERRED = TEMP.TRANSFERRED,
                        MAIN.CONFERENCE = TEMP.CONFERENCE,
                        MAIN.ABNCALLS = TEMP.ABNCALLS,
                        MAIN.ABNTIME = TEMP.ABNTIME,
                        MAIN.I_RINGTIME = TEMP.I_RINGTIME,
                        MAIN.I_DA_ACDTIME = TEMP.I_DA_ACDTIME,
                        MAIN.I_DA_ACWTIME = TEMP.I_DA_ACWTIME,
                        MAIN.DA_ACDTIME = TEMP.DA_ACDTIME,
                        MAIN.DA_ACWTIME = TEMP.DA_ACWTIME,
                        MAIN.DA_OTHERCALLS = TEMP.DA_OTHERCALLS,
                        MAIN.DA_OTHERTIME = TEMP.DA_OTHERTIME,
                        MAIN.RINGCALLS = TEMP.RINGCALLS,
                        MAIN.RINGTIME = TEMP.RINGTIME,
                        MAIN.ANSRINGTIME = TEMP.ANSRINGTIME,
                        MAIN.TI_OTHERTIME = TEMP.TI_OTHERTIME,
                        MAIN.DA_ACWINCALLS = TEMP.DA_ACWINCALLS,
                        MAIN.DA_ACWINTIME = TEMP.DA_ACWINTIME,
                        MAIN.DA_ACWOCALLS = TEMP.DA_ACWOCALLS,
                        MAIN.DA_ACWOTIME = TEMP.DA_ACWOTIME,
                        MAIN.DA_ACWOADJCALLS = TEMP.DA_ACWOADJCALLS,
                        MAIN.DA_ACWOOFFCALLS = TEMP.DA_ACWOOFFCALLS,
                        MAIN.DA_ACWOOFFTIME = TEMP.DA_ACWOOFFTIME,
                        MAIN.NOANSREDIR = TEMP.NOANSREDIR,
                        MAIN.INCOMPLETE = TEMP.INCOMPLETE,
                        MAIN.ACDAUXOUTCALLS = TEMP.ACDAUXOUTCALLS,
                        MAIN.I_ACDAUX_OUTTIME = TEMP.I_ACDAUX_OUTTIME,
                        MAIN.I_ACDAUXINTIME = TEMP.I_ACDAUXINTIME,
                        MAIN.I_ACDOTHERTIME = TEMP.I_ACDOTHERTIME,
                        MAIN.PHANTOMABNS = TEMP.PHANTOMABNS,
                        MAIN.I_AUXTIME = TEMP.I_AUXTIME,
                        MAIN.HOLDACDTIME = TEMP.HOLDACDTIME,
                        MAIN.DA_RELEASE = TEMP.DA_RELEASE,
                        MAIN.ACD_RELEASE = TEMP.ACD_RELEASE,
                        MAIN.TI_AUXTIME0 = TEMP.TI_AUXTIME0,
                        MAIN.TI_AUXTIME1 = TEMP.TI_AUXTIME1,
                        MAIN.TI_AUXTIME2 = TEMP.TI_AUXTIME2,
                        MAIN.TI_AUXTIME3 = TEMP.TI_AUXTIME3,
                        MAIN.TI_AUXTIME4 = TEMP.TI_AUXTIME4,
                        MAIN.TI_AUXTIME5 = TEMP.TI_AUXTIME5,
                        MAIN.TI_AUXTIME6 = TEMP.TI_AUXTIME6,
                        MAIN.TI_AUXTIME7 = TEMP.TI_AUXTIME7,
                        MAIN.TI_AUXTIME8 = TEMP.TI_AUXTIME8,
                        MAIN.TI_AUXTIME9 = TEMP.TI_AUXTIME9,
                        MAIN.ACDCALLS_R1 = TEMP.ACDCALLS_R1,
                        MAIN.ACDCALLS_R2 = TEMP.ACDCALLS_R2,
                        MAIN.I_OTHERSTBYTIME = TEMP.I_OTHERSTBYTIME,
                        MAIN.I_AUXSTBYTIME = TEMP.I_AUXSTBYTIME,
                        MAIN.ETL_DTM = TEMP.ETL_DTM
                """
                cur.execute(merge_query)
                conn.commit()

                # truncate_query="TRUNCATE ODS_CMS.DWCT_DAGENT_TEMP"
                # cur.execute(truncate_query)
                # conn.commit()

    @task(task_id='task_DWCT_DSPLIT_c_01')
    def task_DWCT_DSLIT_c_01():
        # 설정
        s3_bucket = 's3://hdhs-dw-mwaa-migdata'
        s3_prefix = 'ODS_CMS/DWCT_DSPLIT'
        BATCH_SIZE = 100000  # 한 파일에 저장할 행 수

        # JDBC Hook 생성
        jdbc_hook = JdbcHook(jdbc_conn_id='conn_informix_locus1', driver_path=informix_jdbc,
                             driver_class='com.informix.jdbc.IfxDriver')
        chunk_index=1

        while True:
            s3_path = f'{s3_bucket}/{s3_prefix}/dsplit_{chunk_index}.csv'
            offset = (chunk_index - 1) * BATCH_SIZE

            query = f"""
                        SELECT skip {offset} FIRST {offset + BATCH_SIZE} {', '.join(dsplit_columns)}
                        FROM DSPLIT
                        WHERE 
                            (ROW_DATE IS NOT NULL AND ACD IS NOT NULL AND SPLIT IS NOT NULL AND ROW_DATE <> '050108')
                            OR (ROW_DATE = '050108' AND ACD = '1')
                            OR (ROW_DATE = '050108' AND ACD = '2' AND SPLIT <> '900') ORDER BY ROW_DATE ASC
                    """
            print(f"offset={offset}, BATCH_SIZE={BATCH_SIZE}, chunk_index={chunk_index}")
            print(f"query={query}")

            with jdbc_hook.get_conn() as conn:
                df = pd.read_sql(query, conn)

            if df.empty:
                print("No more data to process for this table.")
                break

            file_name = "/tmp/dsplit_{chunk_index}.csv"
            df.to_csv(file_name, index=False)
            print(f"Chunk {chunk_index} saved to {file_name}")

            os.system(f"aws s3 cp {file_name} {s3_path}")
            print(f"Uploaded {file_name} to {s3_path}")

            # 로컬 파일 삭제
            os.remove(file_name)
            print(f"Deleted {file_name} from local directory")

            chunk_index += 1


    @task(task_id='task_DWCT_DSLPIT_c_02')
    def task_DWCT_DSLITT_c_02(df, **kwargs):
        # ETL_DTM 열 추가 (현재 시간)
        df['ETL_DTM'] = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul")).strftime(
            '%Y-%m-%d %H:%M:%S')

        # Snowflake 연결
        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')

        # Snowflake 테이블 이름
        snowflake_table = "DWCT_DAGENT_TEMP"
        snowflake_schema = "DW_LOAD_DB.ODS_CMS"

        with snowflake_hook.get_conn as snowflake_conn:
            truncate_query = f"truncate {snowflake_schema}.{snowflake_table}"
            snowflake_conn.execute(truncate_query)
            df.to_sql(snowflake_table, con=snowflake_conn, schema=snowflake_schema, if_exists='append', index=False)

        @task(task_id='task_DWCT_HSPLIT_c_01')
        def task_DWCT_HSLIT_c_01():
            # 설정
            s3_bucket = 's3://hdhs-dw-mwaa-migdata'
            s3_prefix = 'ODS_CMS/DWCT_DSPLIT'
            BATCH_SIZE = 100000  # 한 파일에 저장할 행 수

            # JDBC Hook 생성
            jdbc_hook = JdbcHook(jdbc_conn_id='conn_informix_locus1', driver_path=informix_jdbc,
                                 driver_class='com.informix.jdbc.IfxDriver')
            chunk_index = 1

            while True:
                s3_path = f'{s3_bucket}/{s3_prefix}/dsplit_{chunk_index}.csv'
                offset = (chunk_index - 1) * BATCH_SIZE

                query = f"""
                                SELECT skip {offset} FIRST {offset + BATCH_SIZE} {', '.join(dsplit_columns)}
                                FROM DSPLIT
                                WHERE 
                                    (ROW_DATE IS NOT NULL AND ACD IS NOT NULL AND SPLIT IS NOT NULL AND ROW_DATE <> '050108')
                                    OR (ROW_DATE = '050108' AND ACD = '1')
                                    OR (ROW_DATE = '050108' AND ACD = '2' AND SPLIT <> '900') ORDER BY ROW_DATE ASC
                            """
                print(f"offset={offset}, BATCH_SIZE={BATCH_SIZE}, chunk_index={chunk_index}")
                print(f"query={query}")

                with jdbc_hook.get_conn() as conn:
                    df = pd.read_sql(query, conn)

                if df.empty:
                    print("No more data to process for this table.")
                    break

                file_name = "/tmp/dsplit_{chunk_index}.csv"
                df.to_csv(file_name, index=False)
                print(f"Chunk {chunk_index} saved to {file_name}")

                os.system(f"aws s3 cp {file_name} {s3_path}")
                print(f"Uploaded {file_name} to {s3_path}")

                # 로컬 파일 삭제
                os.remove(file_name)
                print(f"Deleted {file_name} from local directory")

                chunk_index += 1



        # DAG 실행
    dagent_df = task_DWCT_DAGENT_c_01()
    task_DWCT_DAGENT_c_02(dagent_df)

    task_DWCT_DSLIT_c_01() >> task_DWCT_DSLITT_c_02(dagent_df)


