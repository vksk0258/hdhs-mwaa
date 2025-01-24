from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



with DAG(
    dag_id="dag_DD01_0010_PHYS_DIST_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","DD01_0010_DAILY_MAIN"]
) as dag:
    # 타겟이 MSSQL이라 주석처리
    # trigger_dag_CDC_ETC_PHYS_DIST_01 = TriggerDagRunOperator(
    #     task_id='trigger_dag_CDC_ETC_PHYS_DIST_01',
    #     trigger_dag_id='dag_CDC_ETC_PHYS_DIST_01',
    #     trigger_run_id=None,
    #     execution_date='{{data_interval_start}}',
    #     reset_dag_run=True,
    #     wait_for_completion=False,
    #     poke_interval=60,
    #     allowed_states=['success'],
    #     failed_states=None,
    #     trigger_rule="all_done"
    # )

    trigger_dag_CDC_MART_PHYS_DIST_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_MART_PHYS_DIST_01',
        trigger_dag_id='dag_CDC_MART_PHYS_DIST_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_MART_PHYS_DIST_01