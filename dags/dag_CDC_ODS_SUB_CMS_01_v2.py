from airflow import DAG
import os
from airflow.providers.jdbc.hooks.jdbc import JdbcHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from airflow.operators.python import PythonOperator
import pendulum
import datetime
from airflow.models import Variable

informix_jdbc = Variable.get("informix_jdbc")
informix_jdbc_jc = Variable.get("informix_jdbc_jc")

dsplit_columns = ['ROW_DATE, ACD, SPLIT, I_STAFFTIME, I_AVAILTIME, I_ACDTIME, I_ACWTIME, I_ACWOUTTIME, I_ACWINTIME, I_AUXTIME, I_AUXOUTTIME, I_AUXINTIME, I_OTHERTIME, MAXSTAFFED, ACWINCALLS, ACWINTIME, AUXINCALLS, AUXINTIME, ACWOUTCALLS, ACWOUTTIME, ACWOUTOFFCALLS, ACWOUTOFFTIME, ACWOUTADJCALLS, AUXOUTCALLS, AUXOUTTIME, AUXOUTOFFCALLS, AUXOUTOFFTIME, AUXOUTADJCALLS, EVENT1, EVENT2, EVENT3, EVENT4, EVENT5, EVENT6, EVENT7, EVENT8, EVENT9, ASSISTS, INFLOWCALLS, ACDCALLS, ANSTIME, ACDTIME, ACWTIME, O_ACDCALLS, O_ACDTIME, O_ACWTIME, ACDCALLS1, ACDCALLS2, ACDCALLS3, ACDCALLS4, ACDCALLS5, ACDCALLS6, ACDCALLS7, ACDCALLS8, ACDCALLS9, ACDCALLS10, BACKUPCALLS, HOLDCALLS, HOLDTIME, HOLDABNCALLS, TRANSFERRED, CONFERENCE, ABNCALLS, ABNTIME, ABNCALLS1, ABNCALLS2, ABNCALLS3, ABNCALLS4, ABNCALLS5, ABNCALLS6, ABNCALLS7, ABNCALLS8, ABNCALLS9, ABNCALLS10, DEQUECALLS, DEQUETIME, BUSYCALLS, BUSYTIME, DISCCALLS, DISCTIME, OUTFLOWCALLS, OUTFLOWTIME, INTERFLOWCALLS, LOWCALLS, MEDCALLS, HIGHCALLS, TOPCALLS, ACCEPTABLE, SERVICELEVEL, PERIOD1, PERIOD2, PERIOD3, PERIOD4, PERIOD5, PERIOD6, PERIOD7, PERIOD8, PERIOD9, MAXINQUEUE, MAXOCWTIME, CALLSOFFERED, PERIODCHG, SVCLEVELCHG, I_RINGTIME, RINGTIME, RINGCALLS, ABNRINGCALLS, O_ABNCALLS, O_OTHERCALLS, DA_ACWINCALLS, DA_ACWINTIME, DA_ACWOCALLS, DA_ACWOTIME, NOANSREDIR, INCOMPLETE, ACDAUXOUTCALLS, I_ACDAUX_OUTTIME, I_ACDAUXINTIME, I_ACDOTHERTIME, PHANTOMABNS, OTHERCALLS, OTHERTIME, SLVLABNS, SLVLOUTFLOWS, I_ARRIVED, I_AUXTIME0, I_AUXTIME1, I_AUXTIME2, I_AUXTIME3, I_AUXTIME4, I_AUXTIME5, I_AUXTIME6, I_AUXTIME7, I_AUXTIME8, I_AUXTIME9, I_DA_ACDTIME, I_DA_ACWTIME, I_TAVAILTIME, I_TAUXTIME, MAXTOP, I_NORMTIME, I_OL1TIME, I_OL2TIME, I_TOTHERTIME, MAX_TOT_PERCENTS, ACDCALLS_R1, ACDCALLS_R2, I_ACDTIME_R1, I_ACDTIME_R2, I_ACWTIME_R1, I_ACWTIME_R2, I_RINGTIME_R1, I_RINGTIME_R2, I_OTHERTIME_R1, I_OTHERTIME_R2, I_AUXTIME_R1, I_AUXTIME_R2, I_OTHERSTBYTIME_R1, I_OTHERSTBYTIME_R2, I_AUXSTBYTIME_R1, I_AUXSTBYTIME_R2, I_BEHINDTIME, I_AUTORESERVETIME, TARGETPERCENT, TARGETPCTCHG, TARGETSECONDS, TARGETSECCHG, TARGETACDCALLS, TARGETABNS, TARGETOUTFLOWS']
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

def task_DWCT_HSPLIT_c_01 (table,columns, not_null_columns, **kwargs):

    schema, table_name = table.split('.')

    informixdb_hook = JdbcHook(jdbc_conn_id="conn_informix_locus1", driver_path=informix_jdbc,
                               driver_class=informix_jdbc_jc)
    informixdb_connection = informixdb_hook.get_conn()

    # 각 컬럼에 대해 NOT NULL 조건을 생성
    not_null_conditions = " AND ".join([f"{key} IS NOT NULL" for key in not_null_columns])

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE {not_null_conditions}
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    engine = snowflake_hook.get_sqlalchemy_engine()

    df.to_sql(table, con=engine, if_exists='replace', index=False, chunksize=1000000)


    informixdb_connection.close()
    print("커넥션 종료")

def task_DWCT_DSPLIT_c_01(table, columns, **kwargs):

    schema, table_name = table.split('.')

    informixdb_hook = JdbcHook(jdbc_conn_id="conn_informix_locus1", driver_path=informix_jdbc,
                               driver_class=informix_jdbc_jc)
    informixdb_connection = informixdb_hook.get_conn()

    query = f"""
            SELECT {', '.join(columns)}
            FROM {table_name.split('_')[1]}
            WHERE 
            (ROW_DATE IS NOT NULL AND ACD IS NOT NULL AND SPLIT IS NOT NULL AND ROW_DATE <> '050108')
            OR (ROW_DATE = '050108' AND ACD = 1)
            OR (ROW_DATE = '050108' AND ACD = 2 AND SPLIT <> 900);
            """
    print(query)

    df = pd.read_sql(query, informixdb_connection)

    for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
        df[col] = df[col].apply(
            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
        )

    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    engine = snowflake_hook.get_sqlalchemy_engine()

    df.to_sql(table, con=engine, if_exists='replace', index=False, chunksize=1000000)

    informixdb_connection.close()
    print("커넥션 종료")

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_01_v2",
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=2400),
        tags=["현대홈쇼핑","DD01_0030_DAILY_MAIN"]
) as dag:

    task_DWCT_HSPLIT_c_01 = PythonOperator(
        task_id = "task_DWCT_HSPLIT_c_01",
        python_callable=task_DWCT_HSPLIT_c_01,
        op_args=['ODS_CMS.DWCT_HSPLIT_TEMP',hsplit_columns,hsplit_primary_keys]
    )

    task_DWCT_DSPLIT_c_01 = PythonOperator(
        task_id = "task_DWCT_DSPLIT_c_01",
        python_callable=task_DWCT_DSPLIT_c_01,
        op_args=['ODS_CMS.DWCT_DSPLIT_TEMP',dsplit_columns]
    )

    [task_DWCT_HSPLIT_c_01,task_DWCT_DSPLIT_c_01]



