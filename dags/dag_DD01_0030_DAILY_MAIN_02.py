from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import json
import boto3
import os

# S3 정보
S3_BUCKET = "hdhs-dw-mwaa-s3"
S3_PATH = "param/"


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



with DAG(
        dag_id='dag_DD01_0030_DAILY_MAIN_02',
        description='Extract Snowflake data and upload to S3 as JSON',
        schedule_interval=None  # 필요에 따라 스케줄 설정
) as dag:
    task_create_and_upload_json = PythonOperator(
        task_id='create_and_upload_json',
        python_callable=create_and_upload_json
    )

    task_create_and_upload_json
