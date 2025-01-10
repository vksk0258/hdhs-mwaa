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
    """S3에서 Parquet 파일 읽기, 컬럼 이름 변경, 그리고 다시 업로드"""
    s3 = boto3.client('s3')

    for table in TABLE_NAME_LIST:
        schema, table_name = table.split('.')
        date_folder = kwargs['data_interval_start'].strftime('%Y/%m/%d')
        time_identifier = kwargs['data_interval_start'].strftime('%H%M%S')

        s3_key = f"dw/{schema}/{table_name}/{date_folder}/{kwargs['data_interval_start'].strftime('%Y%m%d')}-{time_identifier}.parquet"
        renamed_s3_key = f"dw/{schema}/{table_name}/{date_folder}/{kwargs['data_interval_start'].strftime('%Y%m%d')}-{time_identifier}_renamed.parquet"

        try:
            # S3에서 파일 다운로드
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            print(f"Downloading file from S3: {s3_key}")

            # Parquet 파일 읽기
            df = pd.read_parquet(BytesIO(response['Body'].read()))

            # 컬럼 이름 변경
            df.rename(columns={
                'OPERATION_FLAG': 'Op',
                'LAST_CHG_DTM': 'transact_id'
            }, inplace=True)

            # 변경된 데이터를 S3에 업로드
            buffer = BytesIO()
            df.to_parquet(buffer, engine='pyarrow', index=False)
            buffer.seek(0)

            s3.put_object(Bucket=S3_BUCKET_NAME, Key=renamed_s3_key, Body=buffer.getvalue())
            print(f"Uploaded renamed file to S3: {renamed_s3_key}")

        except s3.exceptions.NoSuchKey:
            print(f"File not found in S3: {s3_key}")
        except Exception as e:
            print(f"Error processing file {s3_key}: {e}")

dag = DAG(
    dag_id='rename_columns_in_parquet_files',
    description='Rename columns in Parquet files stored in S3 and re-upload them',
    start_date=pendulum.datetime(2024, 12, 20, 16, 0, 0, tz="Asia/Seoul"),
    schedule_interval="0 * * * *",
    tags=['parquet', 'rename', 's3']
)

task = PythonOperator(
    task_id='rename_columns_and_upload',
    python_callable=rename_columns_and_upload,
    provide_context=True,
    dag=dag
)
