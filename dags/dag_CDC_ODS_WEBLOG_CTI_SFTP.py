import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from common.notify_error_functions import notify_api_on_error
from airflow.decorators import task
import os
import boto3
import pendulum


KST = pendulum.timezone("Asia/Seoul")

src_columns = ['PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON', 'VDN_DIAL_NO', 'UCID', 'IN_CALL_NO']
tgt_columns = ['UCID', 'ACD', 'PROC_DATE', 'PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON',
               'VDN_DIAL_DVCD', 'VDN_DIAL_NO', 'IN_CALL_NO', 'ETL_DTM']

s3_client = boto3.client('s3')

# S3 경로
bucket = 'hdhs-dw-mwaa-migdata'

seoul_temp_dir = "/tmp/CTI_Temp/seoul/"
cheongju_temp_dir = "/tmp/CTI_Temp/cheongju/"

def s3_key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False

with DAG(
    dag_id="dag_CDC_ODS_WEBLOG_CTI_SFTP",
    schedule_interval="50 1 * * *",
    start_date=pendulum.datetime(2025, 3, 24, tz="Asia/Seoul"),
    dagrun_timeout=timedelta(minutes=4000),
    tags=["현대홈쇼핑", "DD01_0010_DAILY_MAIN", "CTI", 'Flat File',"Scheduled"]
) as dag:

    @task(task_id='seoul_logfile_conv',
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        )
    def seoul_logfile_cov(**kwargs):

        yesterday = kwargs['data_interval_end'].in_tz(KST) - timedelta(days=1)
        yestermmdd = yesterday.strftime('%m%d')

        s3_key_seoul = f"CTI/SEOUL/hmall_{yestermmdd}.dmp"
        seoul_remote_path = f"/csw/file/dw/CTI/SEOUL/hmall_{yestermmdd}.dmp"

        os.makedirs(seoul_temp_dir, exist_ok=True)

        local_file = f"{seoul_temp_dir}hmall_{yestermmdd}.dmp"

        sftp_hook = SFTPHook(ssh_conn_id='conn_sftp_cti_log')
        sftp_client = sftp_hook.get_conn()
        sftp_client.get(seoul_remote_path, local_file)

        s3_client.upload_file(local_file, bucket, s3_key_seoul)

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

    @task(task_id='cheongju_logfile_conv',
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        )
    def cheongju_logfile_cov(**kwargs):

        yesterday = kwargs['data_interval_end'].in_tz(KST) - timedelta(days=1)
        yestermmdd = yesterday.strftime('%m%d')

        s3_key_cheongju = f"CTI/CHEONGJU/hmall_{yestermmdd}.dmp"
        cheongju_remote_path = f"/csw/file/dw/CTI/CHEONGJU/hmall_{yestermmdd}.dmp"

        os.makedirs(cheongju_temp_dir, exist_ok=True)

        local_file = f"{cheongju_temp_dir}hmall_{yestermmdd}.dmp"

        sftp_hook = SFTPHook(ssh_conn_id='conn_sftp_cti_log')
        sftp_client = sftp_hook.get_conn()
        sftp_client.get(cheongju_remote_path, local_file)

        s3_client.upload_file(local_file, bucket, s3_key_cheongju)

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

    @task(task_id='logfile_data_insert',
        provide_context=True,
        on_failure_callback=notify_api_on_error,
        )
    def logfile_data_insert(**kwargs):

        current_time = kwargs['data_interval_end'].in_tz(KST)

        yesteryymmdd = (current_time - timedelta(days=1)).strftime('%y%m%d')

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
            'PROC_TIME': df['PROC_TIME'].str.lstrip().str[:12],  # 변경
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

        snow_hook = SnowflakeHook(snowflake_conn_id='conn_snow_load')
        with snow_hook.get_conn() as snow_connection:
            with snow_connection.cursor() as snow_cursor:
                columns = ",".join(df.columns)
                placeholders = ",".join(["%s"] * len(df.columns))

                insert_query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"

                # 데이터 변환 (tuple로 변환)
                data_tuples = [tuple(row) for row in df.to_numpy()]

                # ✅ 5만건씩 Batch Insert 실행
                batch_size = 50000
                total_rows = len(data_tuples)

                for i in range(0, total_rows, batch_size):
                    batch = data_tuples[i: i + batch_size]
                    snow_cursor.executemany(insert_query, batch)
                    snow_connection.commit()
                    print(f"✅ {min(i + batch_size, total_rows)} / {total_rows}건 INSERT 완료")


    seoul_logfile_cov = seoul_logfile_cov()
    cheongju_logfile_cov = cheongju_logfile_cov()
    logfile_data_insert = logfile_data_insert()

    [seoul_logfile_cov, cheongju_logfile_cov] >> logfile_data_insert
