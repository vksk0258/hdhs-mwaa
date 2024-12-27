import pandas as pd
from airflow import DAG
from airflow.providers.oracle.hooks.oracle import OracleHook  # Oracle 데이터베이스 연결을 위한 Airflow Hook
from datetime import datetime, timedelta  # 날짜 및 시간 처리를 위한 모듈
from airflow.models import Variable  # Airflow 변수 관리
from airflow.decorators import task  # Airflow의 태스크를 정의하는 데코레이터
import os

cti_seoul_dir='s3://hdhs-dw-mwaa-migdata/CTI/SEOUL/'
cti_cheongju_dir='s3://hdhs-dw-mwaa-migdata/CTI/CHEONGJU/'
cti_bak_dir='s3://hdhs-dw-mwaa-migdata/CTI/CTI_bak/'
TMP_DIR = "/tmp/CTI_Temp"

with DAG(
    dag_id="dag_CDC_ODS_WEBLOG_CTI_01",  # DAG의 고유 식별자
    schedule=None,  # DAG의 예약 일정 없음 (수동 실행)
    dagrun_timeout=timedelta(minutes=4000),  # DAG 실행 제한 시간
    tags=["현대홈쇼핑","CTI",'Flat File']  # DAG에 붙일 태그
) as dag:
    @task(task_id='logfile_conv')
    def logfile_cov(**kwargs):
        work_date = kwargs['execution_date'] - timedelta(days=1)
        work_mmdd = work_date.strftime('%m%d')

        os.makedirs(TMP_DIR, exist_ok=True)

        seoul_s3_key = f"{cti_seoul_dir}hmall_{work_mmdd}.dmp"

        res = os.system(f"aws s3 ls {seoul_s3_key} > dev/null 2>&1")

        if res == 0:
            print(f"File {seoul_s3_key} is not exist")
        else:
            seoul_tmp_key = f"{cti_seoul_dir}seoul_hmall_{work_mmdd}.log"
            os.system(f"aws s3 cp {seoul_s3_key} {seoul_tmp_key}")

    logfile_cov()





