import pandas as pd
from airflow import DAG
from datetime import datetime, timedelta
from airflow.decorators import task
import os
import boto3

cti_seoul_dir = 's3://hdhs-dw-mwaa-migdata/CTI/SEOUL/'
cti_cheongju_dir = 's3://hdhs-dw-mwaa-migdata/CTI/CHEONGJU/'
seoul_temp_dir = "/tmp/CTI_Temp/seoul/"
cheongju_temp_dir = "/tmp/CTI_Temp/cheongju/"

src_columns = ['PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON', 'VDN_DIAL_NO', 'UCID', 'IN_CALL_NO']
tgt_columns = ['UCID', 'ACD', 'PROC_DATE', 'PROC_TIME', 'TRED', 'CALL_ID', 'MESG', 'Q_WAIT_TIME', 'STON',
               'VDN_DIAL_DVCD', 'VDN_DIAL_NO', 'IN_CALL_NO', 'ETL_DTM']

s3_client = boto3.client('s3')

def s3_key_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception:
        return False

with DAG(
    dag_id="dag_CDC_ODS_WEBLOG_CTI_01",
    schedule=None,
    dagrun_timeout=timedelta(minutes=4000),
    tags=["현대홈쇼핑", "CTI", 'Flat File']
) as dag:

    @task(task_id='seoul_logfile_conv')
    def seoul_logfile_cov(**kwargs):
        work_date = kwargs['execution_date'] - timedelta(days=1)
        work_mmdd = work_date.strftime('%m%d')
        bucket = 'hdhs-dw-mwaa-migdata'
        key = f"CTI/SEOUL/hmall_{work_mmdd}.dmp"

        if not s3_key_exists(bucket, key):
            raise FileNotFoundError(f"File {key} does not exist in bucket {bucket}")

        local_file = f"{seoul_temp_dir}hmall_{work_mmdd}.dmp"
        os.makedirs(seoul_temp_dir, exist_ok=True)
        os.system(f"aws s3 cp s3://{bucket}/{key} {local_file}")

        try:
            df = pd.read_csv(local_file, encoding='utf-8', header=None)
        except UnicodeDecodeError:
            df = pd.read_csv(local_file, encoding='euc-kr', header=None)

        df = df.iloc[:, :-3]
        df.columns = src_columns
        os.remove(local_file)

        transformed_file = f"{seoul_temp_dir}seoul_transformed_{work_mmdd}.csv"
        df.to_csv(transformed_file, index=False)
        return transformed_file

    @task(task_id='cheongju_logfile_conv')
    def cheongju_logfile_cov(**kwargs):
        work_date = kwargs['execution_date'] - timedelta(days=1)
        work_mmdd = work_date.strftime('%m%d')
        bucket = 'hdhs-dw-mwaa-migdata'
        key = f"CTI/CHEONGJU/hmall_{work_mmdd}.dmp"

        if not s3_key_exists(bucket, key):
            raise FileNotFoundError(f"File {key} does not exist in bucket {bucket}")

        local_file = f"{cheongju_temp_dir}hmall_{work_mmdd}.dmp"
        os.makedirs(cheongju_temp_dir, exist_ok=True)
        os.system(f"aws s3 cp s3://{bucket}/{key} {local_file}")

        try:
            df = pd.read_csv(local_file, encoding='utf-8', header=None)
        except UnicodeDecodeError:
            df = pd.read_csv(local_file, encoding='euc-kr', header=None)

        df = df.iloc[:, :-3]
        df.columns = src_columns
        os.remove(local_file)

        transformed_file = f"{cheongju_temp_dir}cheongju_transformed_{work_mmdd}.csv"
        df.to_csv(transformed_file, index=False)
        return transformed_file


    @task
    def logfile_data_insert(**kwargs):
        work_date = kwargs['execution_date'] - timedelta(days=1)
        work_yyyymmdd = work_date.strftime('%Y%m%d')

        ti = kwargs['ti']
        seoul_file = ti.xcom_pull(task_ids='seoul_logfile_conv')
        cheongju_file = ti.xcom_pull(task_ids='cheongju_logfile_conv')

        # CSV 파일 읽기
        seoul_df = pd.read_csv(seoul_file)
        cheongju_df = pd.read_csv(cheongju_file)

        # ACD 컬럼 추가
        seoul_df['ACD'] = '1'
        cheongju_df['ACD'] = '2'

        # DataFrame 병합
        df = pd.concat([seoul_df, cheongju_df])

        # 데이터 변환
        transformed_data = {
            'UCID': df['UCID'].str[1:21],  # SUBSTR(UCID,2,20)
            'ACD': df['ACD'],  # 고정값
            'PROC_DATE': work_yyyymmdd,  # YYYYMMDD 형식의 날짜
            'PROC_TIME': df['PROC_TIME'].str.lstrip(),  # LTRIM(PROC_TIME)
            'TRED': df['TRED'],  # 그대로 유지
            'CALL_ID': df['CALL_ID'].str[1:6],  # SUBSTR(CALL_ID,2,5)
            'MESG': df['MESG'],  # 그대로 유지
            'Q_WAIT_TIME': df['Q_WAIT_TIME'].apply(
                lambda x: x[1:] if str(x).startswith('Q') else x  # 맨 앞글자가 Q면 Q제거
            ),
            'STON': df['STON'].str[1:5],  # SUBSTR(STON,2,4)
            'VDN_DIAL_DVCD': df['VDN_DIAL_NO'].str[0].str.upper(),  # UPPER(SUBSTR(VDN_DIAL_NO,1,1))
            'VDN_DIAL_NO': df['VDN_DIAL_NO'].str[1:],  # SUBSTR(VDN_DIAL_NO,2,LENGTH(VDN_DIAL_NO))
            'IN_CALL_NO': df['IN_CALL_NO'].str[1:],  # SUBSTR(IN_CALL_NO,2,LENGTH(IN_CALL_NO))
            'ETL_DTM': kwargs['execution_date'].strftime('%Y-%m-%d %H:%M:%S')  # 문자열로 변환
        }

        # DataFrame 생성
        target_df = pd.DataFrame(transformed_data, columns=tgt_columns)

        # 파일 저장 및 업로드
        output_file = "/tmp/CTI_Temp/cti_temp.csv"
        target_df.to_csv(output_file, index=False, header=True)
        os.system(f"aws s3 cp {output_file} s3://hdhs-dw-migdata-s3/cti_temp.csv")
        os.remove(output_file)


    [seoul_logfile_cov(), cheongju_logfile_cov()] >> logfile_data_insert()
