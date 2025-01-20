from airflow import DAG
from operators.oracle_to_s3_initial_load_operator import OracleToS3InitialLoadOperator
import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# DAG 정의
with DAG(
        dag_id="dag_TDE_initial_load",  # DAG의 고유 식별자
        schedule=None,  # 예약 일정 없음 (수동 실행)
        catchup=False,  # 과거 데이터 실행 스킵
        dagrun_timeout=datetime.timedelta(minutes=6000),  # DAG 실행 제한 시간
        tags=["현대홈쇼핑", "초기적재", "TDE테이블","oracle",'S3']  # DAG에 붙일 태그
) as dag:

    task_CU_ARS_LDIN_MST_CRYPT_load = OracleToS3InitialLoadOperator(
        task_id = "task_CU_ARS_LDIN_MST_CRYPT_load",
        conn_id = "conn_oracle_main",
        table = "HDHS_CU.CU_ARS_LDIN_MST_CRYPT",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_OD_HPNT_PAY_APRVL_DTL_CRYPT_load = OracleToS3InitialLoadOperator(
        task_id = "task_OD_HPNT_PAY_APRVL_DTL_CRYPT_load",
        conn_id = "conn_oracle_OCI",
        table = "HDHS_OD.OD_HPNT_PAY_APRVL_DTL_CRYPT",
        columns = ["*"],
        batch_size = 1000000,
        retries=10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_OD_CRD_APRVL_LOG_CRYPT_load = OracleToS3InitialLoadOperator(
        task_id = "task_OD_CRD_APRVL_LOG_CRYPT_load",
        conn_id = "conn_oracle_OCI",
        table = "HDHS_OD.OD_CRD_APRVL_LOG_CRYPT",
        columns = ["*"],
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_TEC_CONT_CORP_CRYPT_load = OracleToS3InitialLoadOperator(
        task_id = "task_TEC_CONT_CORP_CRYPT_load",
        conn_id = "conn_oracle_OCI",
        table = "HDHS_ECS.TEC_CONT_CORP_CRYPT",
        columns = ["*"],
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_OD_STLM_INF_CRYPT_load = OracleToS3InitialLoadOperator(
        task_id = "task_OD_STLM_INF_CRYPT_load",
        conn_id = "conn_oracle_main",
        table = "HDHS_OD.OD_STLM_INF_CRYPT",
        columns = ["*"],
        retries = 10,
        retry_delay=datetime.timedelta(seconds=10)
    )

    task_CU_ARS_LDIN_MST_CRYPT_load >> task_OD_HPNT_PAY_APRVL_DTL_CRYPT_load >> \
    task_OD_CRD_APRVL_LOG_CRYPT_load >> task_TEC_CONT_CORP_CRYPT_load >> \
    task_OD_STLM_INF_CRYPT_load
