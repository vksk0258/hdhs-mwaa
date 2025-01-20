from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
import pendulum
from airflow.decorators import task
from airflow.models import Variable
import pandas as pd
from datetime import timedelta
import json
import boto3
import os

# 환경 변수 설정
client_path = Variable.get("client_path")

# S3 정보
S3_BUCKET = "hdhs-dw-mwaa-s3"
S3_PATH = "param/"

query = """
SELECT * FROM "DW_ETC"."ETL_SCHEDULE"
"""

update_query = """
MERGE INTO DW_LOAD_DB.DW_ETC.ETL_SCHEDULE AS target
USING (SELECT %s AS FOLDER_NAME, %s AS WORKFLOW_NAME, %s AS PARAM_NAME, %s AS PARAM_ORDER, %s AS RUN_OPTION, %s AS OPTION_VALUE, %s AS FORMAT_DEFAULT, %s AS FORMAT_MM, %s AS FORMAT_DD, %s AS FORMAT_HH, %s AS FORMAT_MI, %s AS FORMAT_SS, %s AS PARAM_VALUE, %s AS ETL_DTM) AS source
ON target.FOLDER_NAME = source.FOLDER_NAME AND target.WORKFLOW_NAME = source.WORKFLOW_NAME AND target.PARAM_NAME = source.PARAM_NAME
WHEN MATCHED THEN
    UPDATE SET
        PARAM_ORDER = source.PARAM_ORDER,
        RUN_OPTION = source.RUN_OPTION,
        OPTION_VALUE = source.OPTION_VALUE,
        FORMAT_DEFAULT = source.FORMAT_DEFAULT,
        FORMAT_MM = source.FORMAT_MM,
        FORMAT_DD = source.FORMAT_DD,
        FORMAT_HH = source.FORMAT_HH,
        FORMAT_MI = source.FORMAT_MI,
        FORMAT_SS = source.FORMAT_SS,
        PARAM_VALUE = source.PARAM_VALUE,
        ETL_DTM = source.ETL_DTM
WHEN NOT MATCHED THEN
    INSERT (FOLDER_NAME, WORKFLOW_NAME, PARAM_NAME, PARAM_ORDER, RUN_OPTION, OPTION_VALUE, FORMAT_DEFAULT, FORMAT_MM, FORMAT_DD, FORMAT_HH, FORMAT_MI, FORMAT_SS, PARAM_VALUE, ETL_DTM)
    VALUES (source.FOLDER_NAME, source.WORKFLOW_NAME, source.PARAM_NAME, source.PARAM_ORDER, source.RUN_OPTION, source.OPTION_VALUE, source.FORMAT_DEFAULT, source.FORMAT_MM, source.FORMAT_DD, source.FORMAT_HH, source.FORMAT_MI, source.FORMAT_SS, source.PARAM_VALUE, source.ETL_DTM)
"""

def calculate_param_value(row, sysdate):
    run_option = row["RUN_OPTION"]
    option_value = row["OPTION_VALUE"] if pd.notnull(row["OPTION_VALUE"]) else 0
    value_date = sysdate

    # FORMAT 값 적용
    try:
        if pd.notnull(row["FORMAT_MM"]):
            value_date = value_date.replace(month=int(row["FORMAT_MM"]))
        if pd.notnull(row["FORMAT_DD"]):
            value_date = value_date.replace(day=int(row["FORMAT_DD"]))
        if pd.notnull(row["FORMAT_HH"]):
            value_date = value_date.replace(hour=int(row["FORMAT_HH"]))
        if pd.notnull(row["FORMAT_MI"]):
            value_date = value_date.replace(minute=int(row["FORMAT_MI"]))
        if pd.notnull(row["FORMAT_SS"]):
            value_date = value_date.replace(second=int(row["FORMAT_SS"]))
    except ValueError as e:
        print(f"Invalid date adjustment: {e}")
        raise

    # 옵션 값 기반 시간 조정
    if run_option == 'YY':
        value_date = value_date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=option_value * 365)
    elif run_option == 'MM':
        value_date = value_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0) + timedelta(days=option_value * 30)
    elif run_option == 'DD':
        value_date = value_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=option_value)
    elif run_option == 'HH':
        value_date = value_date.replace(minute=0, second=0, microsecond=0) + timedelta(hours=option_value)
    elif run_option == 'MI':
        value_date = value_date.replace(second=0, microsecond=0) + timedelta(minutes=option_value)
    else:
        value_date += timedelta(seconds=option_value)

    # PARAM_VALUE 형식 지정 및 추가 FORMAT 값 반영
    format_map = {
        'YY': '%Y',
        'MM': '%Y%m',
        'DD': '%Y%m%d',
        'HH': '%Y%m%d%H',
        'MI': '%Y%m%d%H%M'
    }
    param_value = value_date.strftime(format_map.get(run_option, '%Y%m%d%H%M%S'))

    # 추가 FORMAT 값 연결
    additional_formats = []
    if pd.notnull(row["FORMAT_MM"]):
        additional_formats.append(row['FORMAT_MM'])
    if pd.notnull(row["FORMAT_DD"]):
        additional_formats.append(row['FORMAT_DD'])
    if pd.notnull(row["FORMAT_HH"]):
        additional_formats.append(row['FORMAT_HH'])
    if pd.notnull(row["FORMAT_MI"]):
        additional_formats.append(row['FORMAT_MI'])
    if pd.notnull(row["FORMAT_SS"]):
        additional_formats.append(row['FORMAT_SS'])

    if additional_formats:
        param_value += ''.join(additional_formats)

    return param_value

# Snowflake에서 데이터를 가져오는 함수
def fetch_data_from_snowflake():
    query = f"SELECT WORKFLOW_NAME, PARAM_NAME, PARAM_VALUE FROM DW_LOAD_DB.DW_ETC.ETL_SCHEDULE"
    snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
    with snowflake_hook.get_conn() as snowflake_conn:
        cursor = snowflake_conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        snowflake_conn.close()
        return results


# JSON 파일 생성 및 S3 업로드 함수
def create_and_upload_json(**kwargs):
    data = fetch_data_from_snowflake()

    # 워크플로우 별로 데이터를 정리
    workflow_data = {}
    for workflow_name, param_name, param_value in data:
        if workflow_name not in workflow_data:
            workflow_data[workflow_name] = {}
        workflow_data[workflow_name][param_name] = param_value

    # JSON 파일 생성 및 S3 업로드
    s3_client = boto3.client('s3')
    for workflow_name, params in workflow_data.items():
        json_file_name = f"{workflow_name}.json"
        local_path = f"/tmp/{json_file_name}"

        # JSON 파일 저장
        with open(local_path, "w") as json_file:
            json.dump(params, json_file)

        # S3에 업로드
        s3_client.upload_file(local_path, S3_BUCKET, f"{S3_PATH}{json_file_name}")

        # 로컬 파일 삭제
        os.remove(local_path)


# DAG 정의
with DAG(
        dag_id="dag_DD01_0010_DAILY_MAIN_01",
        schedule_interval='0 2 * * *',
        start_date=pendulum.datetime(2025, 1, 18, tz="Asia/Seoul"),
        catchup=False,
        tags=["현대홈쇼핑", "Daily"]
) as dag:
    @task(task_id="task_ETL_SCHEDULE_c_01")
    def task_ETL_SCHEDULE_c_01(**kwargs):
        sysdate = kwargs['data_interval_end'].in_tz(pendulum.timezone("Asia/Seoul"))
        oracle_hook = OracleHook(oracle_conn_id="conn_oracle_H2O", thick_mode=True, thick_mode_lib_dir=client_path)
        with oracle_hook.get_conn() as oracle_conn:
            df = pd.read_sql(query, oracle_conn)

            # PARAM_VALUE 및 ETL_DTM 계산
            df["PARAM_VALUE"] = df.apply(calculate_param_value, axis=1, sysdate=sysdate)
            df["ETL_DTM"] = sysdate.strftime('%Y-%m-%d %H:%M:%S')

            # OPTION_VALUE가 NULL인 경우 0으로 설정
            df["OPTION_VALUE"] = df["OPTION_VALUE"].apply(lambda x: 0 if pd.isnull(x) else x)

            # NaN 값을 None으로 변경 (Snowflake에서 NULL로 삽입)
            df = df.where(pd.notnull(df), None)

            # 데이터 타입을 명시적으로 지정하여 문자열 유지
            for col in ["FORMAT_MM", "FORMAT_DD", "FORMAT_HH", "FORMAT_MI", "FORMAT_SS"]:
                df[col] = df[col].apply(lambda x: x if x is not None else None)

        snowflake_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        with snowflake_hook.get_conn() as snowflake_conn:
            try:
                cursor = snowflake_conn.cursor()

                # DataFrame 데이터를 튜플 리스트로 변환
                data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]

                # 디버깅: 업데이트하려는 데이터 확인
                print("Data to be updated:", data_tuples)

                for data_tuple in data_tuples:
                    cursor.execute(update_query, data_tuple)

                snowflake_conn.commit()
                print("Data updated successfully!")
            except Exception as e:
                print(f"Error updating data: {e}")
            finally:
                snowflake_conn.close()

    task_create_and_upload_json = PythonOperator(
        task_id='create_and_upload_json',
        python_callable=create_and_upload_json
    )

    # trigger_dag_CDC_MART_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_MART_01',
    #     trigger_dag_id='dag_CDC_MART_01',
    #     trigger_run_id=None,
    #     reset_dag_run=True,
    #     wait_for_completion=False,
    #     poke_interval=60,
    #     allowed_states=['success'],
    #     failed_states=None,
    #     trigger_rule="all_done"
    # )

    trigger_dag_CDC_ODS_SUB_ALLI_01_S3 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_SUB_ALLI_01_S3',
        trigger_dag_id='dag_CDC_ODS_SUB_ALLI_01_S3',
        trigger_run_id=None,
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )

    trigger_dag_CDC_ODS_SUB_CMS_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_ODS_SUB_CMS_01',
        trigger_dag_id='dag_CDC_ODS_SUB_CMS_01',
        trigger_run_id=None,
        reset_dag_run=True,
        wait_for_completion=False,
        poke_interval=60,
        allowed_states=['success'],
        failed_states=None,
        trigger_rule="all_done"
    )



    task_ETL_SCHEDULE_c_01() >> task_create_and_upload_json >> trigger_dag_CDC_ODS_SUB_ALLI_01_S3 >> trigger_dag_CDC_ODS_SUB_CMS_01
