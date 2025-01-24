from airflow import DAG
from operators.oracle_to_s3_initial_load_operator import OracleToS3InitialLoadOperator
import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
        dag_id="dag_CDC_ODS_SUB_CMS_initianl_load",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "초기적재","CMS", "ODS","oracle",'S3']  # DAG에 붙일 태그
) as dag:

    task_DWCT_DAGENT_load = OracleToS3InitialLoadOperator(
        task_id = "task_DWCT_DAGENT_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_CMS.DWCT_DAGENT",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_DWCT_DSPLIT_load = OracleToS3InitialLoadOperator(
        task_id = "task_DWCT_DSPLIT_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_CMS.DWCT_DSPLIT",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_DWCT_HAGENT_load = OracleToS3InitialLoadOperator(
        task_id = "task_DWCT_HAGENT_load",
        conn_id = "conn_oracle_OCI",
        table = "ODS_CMS.DWCT_HAGENT",
        columns = ["*"],
        batch_size=1000000,
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_DWCT_HSPLIT_load = OracleToS3InitialLoadOperator(
        task_id="task_DWCT_HSPLIT_load",
        conn_id="conn_oracle_OCI",
        table="ODS_CMS.DWCT_HSPLIT",
        columns=["*"],
        batch_size=1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_DWCT_DAGENT_load >> task_DWCT_DSPLIT_load >> \
    task_DWCT_HAGENT_load >> task_DWCT_HSPLIT_load