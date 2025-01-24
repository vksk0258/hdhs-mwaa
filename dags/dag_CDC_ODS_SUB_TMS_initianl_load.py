from airflow import DAG
from operators.oracle_to_s3_initial_load_operator import OracleToS3InitialLoadOperator
import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_TMS_initianl_load",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "초기적재","TMS", "ODS","oracle",'S3']  # DAG에 붙일 태그
) as dag:

    task_TMS_APP_DEVICE_LIST_load = OracleToS3InitialLoadOperator(
        task_id = "task_TMS_APP_DEVICE_LIST_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_TMS.TMS_APP_DEVICE_LIST",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TMS_APP_USER_LIST_load = OracleToS3InitialLoadOperator(
        task_id = "task_TMS_APP_USER_LIST_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_TMS.TMS_APP_USER_LIST",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TMS_SITE_USER_LIST_load = OracleToS3InitialLoadOperator(
        task_id = "task_TMS_SITE_USER_LIST_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_TMS.TMS_SITE_USER_LIST",
        columns = ["*"],
        batch_size=1000000,
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TMS_APP_DEVICE_LIST_load >> task_TMS_APP_USER_LIST_load >> \
    task_TMS_SITE_USER_LIST_load