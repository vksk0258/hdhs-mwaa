from airflow import DAG
import datetime
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

with DAG(
    dag_id="dags_empty_with_edge_lable",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'python','라벨']
) as dag:
    empty_1 = EmptyOperator(task_id="Empty_1")

    empty_2 = EmptyOperator(task_id="Empty_2")

    empty_1 >> Label('1과 2사이') >> empty_2

    empty_3 = EmptyOperator(task_id="Empty_3")

    empty_4 = EmptyOperator(task_id="Empty_4")

    empty_5 = EmptyOperator(task_id="Empty_5")

    empty_6 = EmptyOperator(task_id="Empty_6")

    empty_2 >> Label('start') >> [empty_3, empty_4, empty_5] >> Label('end') >> empty_6