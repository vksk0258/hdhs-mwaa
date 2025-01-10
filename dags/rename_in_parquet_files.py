from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
import pandas as pd
import pendulum
import datetime
from io import BytesIO

KST = pendulum.timezone("Asia/Seoul")

# 환경 설정
S3_BUCKET_NAME = "hdhs-dw-migdata-s3"
TABLE_NAME_LIST = [
    "HDHS_OD.OD_STLM_INF_CRYPT",
    "HDHS_CU.CU_ARS_LDIN_MST_CRYPT",
    "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
    "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
    "HDHS_ECS.TEC_CONT_CORP_CRYPT"
]

def rename_columns_and_upload(**kwargs):
    """S3에서 Parquet 파일 읽기, 컬럼 이름 변경, 기존 파일 삭제, 그리고 이름 변경"""
    s3 = boto3.client('s3')

    for table in TABLE_NAME_LIST:
        schema, table_name = table.split('.')
        date_folder = kwargs['data_interval_start'].strftime('%Y/%m/%d')
        time_identifier = kwargs['data_interval_start'].strftime('%H%M%S')

        s3_key = f"dw/{schema}/{table_name}/{date_folder}/{kwargs['data_interval_start'].strftime('%Y%m%d')}-{time_identifier}.parquet"
        renamed_s3_key = f"dw/{schema}/{table_name}/{date_folder}/{kwargs['data_interval_start'].strftime('%Y%m%d')}-{time_identifier}_renamed.parquet"

        try:
            # 기존 파일 삭제
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            print(f"Deleted original file from S3: {s3_key}")

            # 파일 이름 변경
            copy_source = {'Bucket': S3_BUCKET_NAME, 'Key': renamed_s3_key}
            s3.copy_object(Bucket=S3_BUCKET_NAME, CopySource=copy_source, Key=s3_key)
            s3.delete_object(Bucket=S3_BUCKET_NAME, Key=renamed_s3_key)
            print(f"Renamed {renamed_s3_key} to {s3_key}")

        except s3.exceptions.NoSuchKey:
            print(f"File not found in S3: {s3_key}")
        except Exception as e:
            print(f"Error processing file {s3_key}: {e}")

dag = DAG(
    dag_id='rename_in_parquet_files',
    description='Rename columns in Parquet files stored in S3 and re-upload them',
    start_date=pendulum.datetime(2024, 12, 25, 16, 0, 0, tz="Asia/Seoul"),
    schedule_interval="0 * * * *",
    tags=['parquet', 'rename', 's3']
)

task = PythonOperator(
    task_id='rename_columns_and_upload',
    python_callable=rename_columns_and_upload,
    provide_context=True,
    dag=dag
)
