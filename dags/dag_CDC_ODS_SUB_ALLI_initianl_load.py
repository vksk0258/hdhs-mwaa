from airflow import DAG
from operators.oracle_to_s3_initial_load_operator import OracleToS3InitialLoadOperator
import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_ALLI_initianl_load",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "초기적재","ALLI", "ODS","oracle",'S3']  # DAG에 붙일 태그
) as dag:

    task_AM_ALML_MD_VEN_INTL_SETUP_DTL_load = OracleToS3InitialLoadOperator(
        task_id = "task_AM_ALML_MD_VEN_INTL_SETUP_DTL_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_ALLI.AM_ALML_MD_VEN_INTL_SETUP_DTL",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_AM_ALML_INTL_EXCP_SETUP_DTL_load = OracleToS3InitialLoadOperator(
        task_id = "task_AM_ALML_INTL_EXCP_SETUP_DTL_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_TMS.ODS_ALLI.AM_ALML_INTL_EXCP_SETUP_DTL",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_AM_ALML_ITEM_INTL_DTL_load = OracleToS3InitialLoadOperator(
        task_id = "task_AM_ALML_ITEM_INTL_DTL_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_ALLI.AM_ALML_ITEM_INTL_DTL",
        columns = ["*"],
        batch_size=1000000,
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_AM_ALML_MD_VEN_INTL_SETUP_DTL_load >> task_AM_ALML_INTL_EXCP_SETUP_DTL_load >> \
    task_AM_ALML_ITEM_INTL_DTL_load