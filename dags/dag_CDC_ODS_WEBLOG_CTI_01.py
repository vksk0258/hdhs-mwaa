import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
import os
import boto3
import pendulum


KST = pendulum.timezone("Asia/Seoul")

cti_seoul_dir = 's3://hdhs-dw-mwaa-migdata/CTI/SEOUL/'
cti_cheongju_dir = 's3://hdhs-dw-mwaa-migdata/CTI/CHEONGJU/'
seoul_temp_dir = "/tmp/CTI_Temp/seoul/"
cheongju_temp_dir = "/tmp/CTI_Temp/cheongju/"

S3_BUCKET_NAME = "hdhs-dw-migdata-s3"

src_columns = ['PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON', 'VDN_DIAL_NO', 'UCID', 'IN_CALL_NO']
tgt_columns = ['UCID', 'ACD', 'PROC_DATE', 'PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON',
               'VDN_DIAL_DVCD', 'VDN_DIAL_NO', 'IN_CALL_NO', 'ETL_DTM']

s3_client = boto3.client('s3')

def s3_key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False

with DAG(
    dag_id="dag_CDC_ODS_WEBLOG_CTI_01",
    schedule_interval='30 0 * * *',
    start_date=pendulum.datetime(2025, 2, 23, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    tags=["현대홈쇼핑", "DD01_0010_DAILY_MAIN", "CTI", 'Flat File']
) as dag:
    @task(task_id='seoul_logfile_conv')
    def seoul_logfile_cov(**kwargs):
        yesterday = kwargs['data_interval_end'].in_tz(KST) - timedelta(days=1)
        yestermmdd = yesterday.strftime('%m%d')
        bucket = 'hdhs-dw-mwaa-migdata'
        key = f"CTI/SEOUL/hmall_{yestermmdd}.dmp"

        if not s3_key_exists(bucket, key):
            raise FileNotFoundError(f"File {key} does not exist in bucket {bucket}")

        local_file = f"{seoul_temp_dir}hmall_{yestermmdd}.dmp"
        os.makedirs(seoul_temp_dir, exist_ok=True)
        s3_client.download_file(bucket, key, local_file)

        # 시도할 인코딩 리스트
        encodings_to_try = ['utf-8', 'euc-kr', 'cp949', 'latin1']

        for encoding in encodings_to_try:
            try:
                print(f"Trying encoding: {encoding}")
                df = pd.read_csv(local_file, encoding=encoding, header=None, on_bad_lines='skip', engine='python')
                print(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                print(f"Failed to read file with encoding: {encoding}")
                continue
        else:
            # 모든 인코딩 시도가 실패하면 에러 발생
            raise ValueError(f"Unable to read the file {local_file} with attempted encodings: {encodings_to_try}")

        # 데이터 전처리
        df = df.iloc[:, :-3]
        df.columns = src_columns
        os.remove(local_file)

        # Define S3 target path
        s3_output_path = f"CTI/tmp/seoul_transformed_{yestermmdd}.parquet"

        # Upload transformed data directly to S3
        transformed_file = f"{seoul_temp_dir}seoul_transformed_{yestermmdd}.parquet"
        df.to_parquet(transformed_file, engine='pyarrow', index=False)
        s3_client.upload_file(transformed_file, bucket, s3_output_path)
        os.remove(transformed_file)

        # Return the S3 path for downstream tasks
        return f"s3://{bucket}/{s3_output_path}"


    @task(task_id='cheongju_logfile_conv')
    def cheongju_logfile_cov(**kwargs):
        yesterday = kwargs['data_interval_end'].in_tz(KST) - timedelta(days=1)
        yestermmdd = yesterday.strftime('%m%d')
        bucket = 'hdhs-dw-mwaa-migdata'
        key = f"CTI/CHEONGJU/hmall_{yestermmdd}.dmp"

        if not s3_key_exists(bucket, key):
            raise FileNotFoundError(f"File {key} does not exist in bucket {bucket}")

        local_file = f"{cheongju_temp_dir}hmall_{yestermmdd}.dmp"
        os.makedirs(cheongju_temp_dir, exist_ok=True)
        s3_client.download_file(bucket, key, local_file)

        # 시도할 인코딩 리스트
        encodings_to_try = ['utf-8', 'euc-kr', 'cp949', 'latin1']

        for encoding in encodings_to_try:
            try:
                print(f"Trying encoding: {encoding}")
                df = pd.read_csv(local_file, encoding=encoding, header=None, on_bad_lines='skip', engine='python')
                print(f"Successfully read file with encoding: {encoding}")
                break
            except UnicodeDecodeError:
                print(f"Failed to read file with encoding: {encoding}")
                continue
        else:
            # 모든 인코딩 시도가 실패하면 에러 발생
            raise ValueError(f"Unable to read the file {local_file} with attempted encodings: {encodings_to_try}")

        # 데이터 전처리
        df = df.iloc[:, :-3]
        df.columns = src_columns
        os.remove(local_file)

        # Define S3 target path
        s3_output_path = f"CTI/tmp/cheongju_transformed_{yestermmdd}.parquet"

        # Upload transformed data directly to S3
        transformed_file = f"{cheongju_temp_dir}cheongju_transformed_{yestermmdd}.parquet"
        df.to_parquet(transformed_file, engine='pyarrow', index=False)
        s3_client.upload_file(transformed_file, bucket, s3_output_path)
        os.remove(transformed_file)

        # Return the S3 path for downstream tasks
        return f"s3://{bucket}/{s3_output_path}"

    @task
    def logfile_data_insert(**kwargs):
        TMP_DIR = "/tmp/CTI_incremental"
        current_time = kwargs['data_interval_end'].in_tz(KST)

        yesteryymmdd = (current_time - timedelta(days=1)).strftime('%y%m%d')

        date_folder = current_time.strftime('%Y/%m/%d')
        time_identifier = current_time.strftime('%H%M%S')

        schema = "ODS_CTI"
        table_name = "DWCT_CTI_REAL"


        seoul_s3_path = kwargs['ti'].xcom_pull(task_ids='seoul_logfile_conv')
        cheongju_s3_path = kwargs['ti'].xcom_pull(task_ids='cheongju_logfile_conv')

        # Define local temporary file paths
        local_seoul_file = '/tmp/seoul_transformed.parquet'
        local_cheongju_file = '/tmp/cheongju_transformed.parquet'

        # Download files from S3 to local
        s3_client.download_file('hdhs-dw-mwaa-migdata', seoul_s3_path.replace('s3://hdhs-dw-mwaa-migdata/', ''),
                                local_seoul_file)
        s3_client.download_file('hdhs-dw-mwaa-migdata', cheongju_s3_path.replace('s3://hdhs-dw-mwaa-migdata/', ''),
                                local_cheongju_file)

        # Read the local Parquet files
        seoul_df = pd.read_parquet(local_seoul_file, engine='pyarrow')
        cheongju_df = pd.read_parquet(local_cheongju_file, engine='pyarrow')

        # Remove local files after reading
        os.remove(local_seoul_file)
        os.remove(local_cheongju_file)

        # Example processing logic (replace with actual logic as needed)
        print("Seoul DataFrame:", seoul_df.head())
        print("Cheongju DataFrame:", cheongju_df.head())

        # Further processing or insertion into database can be added here

        seoul_df['ACD'] = '1'
        cheongju_df['ACD'] = '2'

        df = pd.concat([seoul_df, cheongju_df])

        transformed_data = {
            'UCID': df['UCID'].str[1:21],
            'ACD': df['ACD'],
            'PROC_DATE': yesteryymmdd,
            'PROC_TIME': df['PROC_TIME'].str.lstrip(),
            'TRED': df['TRED'],
            'CALL_ID': df['CALL_ID'].str[1:6],
            'MESG': df['MESG'],
            'Q_WAIT_TIME': df['Q_WAIT_TIME'].apply(lambda x: x[1:] if str(x).startswith('Q') else x),
            'STON': df['STON'].str[1:5],
            'VDN_DIAL_DVCD': df['VDN_DIAL_NO'].str[0].str.upper(),
            'VDN_DIAL_NO': df['VDN_DIAL_NO'].str[1:],
            'IN_CALL_NO': df['IN_CALL_NO'].str[1:],
            'ETL_DTM': pendulum.now(KST).strftime('%Y-%m-%d %H:%M:%S')
        }

        df = pd.DataFrame(transformed_data, columns=tgt_columns)

        if not df.empty:
            file_name = f"{TMP_DIR}/{table_name}_{current_time.strftime('%Y%m%d')}-{time_identifier}.parquet"

            os.makedirs(TMP_DIR, exist_ok=True)
            df.to_parquet(file_name, engine='pyarrow', index=False)
            s3_client.upload_file(file_name, S3_BUCKET_NAME, f"dw/mwaa_etl_load/{schema}/{table_name}/{date_folder}/{file_name.split('/')[-1]}")
            os.remove(file_name)
        else:
            print(f"No data to process for table {schema}.{table_name} in the given time window.")

    [seoul_logfile_cov(),cheongju_logfile_cov()] >> logfile_data_insert()
