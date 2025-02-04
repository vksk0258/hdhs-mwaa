from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator



with DAG(
    dag_id="dag_DD01_0800_DPRCH_DASH_BOARD_01",
    schedule_interval=None,
    tags=["현대홈쇼핑","100_COM"]
) as dag:
    trigger_dag_CDC_META_DPRCH_DASH_01 = TriggerDagRunOperator(
        task_id='trigger_dag_CDC_META_DPRCH_DASH_01',
        trigger_dag_id='dag_CDC_META_DPRCH_DASH_01',
        reset_dag_run=True,         # 이미 수행된 dag여도 수행 할 것인지
        wait_for_completion=True,  # 트리거 하는 dag가 끝날때까지 기다릴 것인지
        poke_interval=60,
        allowed_states=['success'], # 트리거 하는 dag가 어떤 상태여야 오퍼레이터가 성공으로 끝나는지
        trigger_rule="all_done"
    )

    trigger_dag_CDC_META_DPRCH_DASH_01