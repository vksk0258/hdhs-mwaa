from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    # 이덱은 매일 6시 30분에 시작
    schedule="30 6 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'python','트리거']
) as dag:
    @task.branch(task_id='branching')
    def random_branch():
        import random

        item_list = ['A', 'B', 'C']
        selected_items = random.choice(item_list)
        if selected_items == 'A':
            return 'task_a'
        elif selected_items == 'B':
            return 'task_b'
        elif selected_items == 'C':
            return 'task_c'


    task_a = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1'
    )

    @task(task_id='task_b')
    def task_b():
        print('정상처리')

    @task(task_id='task_c')
    def task_c():
        print('정상처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상처리')


    random_branch() >> [task_a, task_b(), task_c()] >> task_d()