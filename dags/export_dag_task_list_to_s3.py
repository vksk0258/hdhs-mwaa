from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import DagBag
import csv
import os
import boto3
from airflow.hooks.base import BaseHook

# S3 설정
S3_BUCKET = 'hdhs-dw-mwaa-s3'
S3_KEY = 'backup/meta/'

# ✅ /tmp/에 저장할 디렉토리 설정 (쓰기 가능한 경로)
LOCAL_OUTPUT_DIR = "/tmp/airflow_output"
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)  # /tmp 경로는 보통 쓰기 가능

# CSV 파일 경로
DAG_CSV_FILE = os.path.join(LOCAL_OUTPUT_DIR, "dag_list.csv")
TASK_CSV_FILE = os.path.join(LOCAL_OUTPUT_DIR, "dag_task_list.csv")


def get_dag_list():
    """DAG 리스트를 CSV 파일로 저장"""
    dag_bag = DagBag()
    dag_list = [(dag_id, dag_bag.get_dag(dag_id).fileloc) for dag_id in dag_bag.dags]

    with open(DAG_CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["dag_id", "file_location"])  # 헤더 추가
        writer.writerows(dag_list)

    print(f"✅ DAG 리스트 저장 완료: {DAG_CSV_FILE}")


def get_task_list_per_dag():
    """DAG별 TASK 리스트를 CSV 파일로 저장"""
    dag_bag = DagBag()
    task_list = []

    for dag_id, dag in dag_bag.dags.items():
        for task in dag.tasks:
            task_list.append((dag_id, task.task_id, task.task_type))

    with open(TASK_CSV_FILE, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(["dag_id", "task_id", "task_type"])  # 헤더 추가
        writer.writerows(task_list)

    print(f"✅ TASK 리스트 저장 완료: {TASK_CSV_FILE}")


def upload_to_s3():
    """CSV 파일을 S3로 업로드"""
    s3_client = boto3.client(
        "s3")

    # 업로드할 파일 리스트
    files_to_upload = [
        (DAG_CSV_FILE, f"{S3_KEY}dag_list.csv"),
        (TASK_CSV_FILE, f"{S3_KEY}dag_task_list.csv"),
    ]

    for local_file, s3_path in files_to_upload:
        if os.path.exists(local_file) and os.path.isfile(local_file):  # 파일인지 확인
            s3_client.upload_file(local_file, S3_BUCKET, s3_path)
            print(f"✅ 업로드 완료: {local_file} → s3://{S3_BUCKET}/{s3_path}")
        else:
            print(f"❌ 파일 없음: {local_file}, 업로드 생략")


with DAG(
        dag_id="export_dag_task_list_to_s3",
        schedule_interval=None,  # 매일 실행
        catchup=False,
        tags=["monitoring"],
) as dag:
    task_get_dag_list = PythonOperator(
        task_id="get_dag_list",
        python_callable=get_dag_list
    )

    task_get_task_list = PythonOperator(
        task_id="get_task_list_per_dag",
        python_callable=get_task_list_per_dag
    )

    task_upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    # DAG 실행 순서
    task_get_dag_list >> task_get_task_list >> task_upload_s3
