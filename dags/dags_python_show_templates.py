from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task

with DAG(
    dag_id="dags_python_show_templates",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'python']

    
) as dag:
    @task(task_id="print_task")
    def show_templates(**kwargs):
        from pprint import pprint
        pprint(kwargs)

    show_templates()