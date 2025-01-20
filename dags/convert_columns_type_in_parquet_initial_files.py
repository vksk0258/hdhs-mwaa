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
    "HDHS_OD.OD_STLM_INF_CRYPT"
    # "HDHS_CU.CU_ARS_LDIN_MST_CRYPT",
    # "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
    # "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
    # "HDHS_ECS.TEC_CONT_CORP_CRYPT"
]

def rename_columns_and_upload(**kwargs):
    """S3에서 Parquet 파일 읽기, 컬럼 이름 변경, 그리고 다시 업로드"""
    s3 = boto3.client('s3')

    for table in TABLE_NAME_LIST:
        schema, table_name = table.split('.')
        s3_prefix = f"dw/{schema}/{table_name}/"

        try:
            # 지정된 경로에서 모든 Parquet 파일 리스트 가져오기
            response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=s3_prefix)

            if 'Contents' not in response:
                print(f"No files found in S3 with prefix: {s3_prefix}")
                continue

            for obj in response['Contents']:
                s3_key = obj['Key']
                if not s3_key.endswith('.parquet'):
                    continue

                print(f"Processing file: {s3_key}")

                # S3에서 파일 다운로드
                file_response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                df = pd.read_parquet(BytesIO(file_response['Body'].read()))

                # 컬럼 리스트 정의
                target_columns = ['APRVL_DTM', 'CHG_DTM', 'CNCL_DTM', 'LAST_APRVL_DTM', 'REG_DTM', 'STLM_DTM',
                                  'STLM_PRRG_DTM']

                # 조건 적용
                for col in target_columns:
                    if col in df.columns:  # 데이터프레임에 해당 컬럼이 있는지 확인
                        df[col] = df[col].apply(
                            lambda x: None if pd.isnull(x) or x == pd.NaT or str(x).strip() in ['NaT', '']
                            else x.isoformat() if isinstance(x, pd.Timestamp) else str(x)
                        )

                # 변경된 데이터를 S3에 업로드
                buffer = BytesIO()
                df.to_parquet(buffer, engine='pyarrow', index=False)
                buffer.seek(0)

                s3.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
                print(f"Uploaded updated file to S3: {s3_key}")

        except Exception as e:
            print(f"Error processing files in prefix {s3_prefix}: {e}")

dag = DAG(
    dag_id='convert_columns_type_in_parquet_initial_files',
    description='Convert date columns to string in Parquet files stored in S3 and re-upload them',
    schedule_interval=None,
    tags=['parquet', 'rename', 's3']
)

task = PythonOperator(
    task_id='rename_columns_and_upload',
    python_callable=rename_columns_and_upload,
    provide_context=True,
    dag=dag
)
