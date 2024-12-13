from airflow import DAG
import datetime
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    # 이덱은 매일 6시 30분에 시작
    schedule=None,
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
        print(selected_items)
        if selected_items == 'A':
            return 'task_a'
        elif selected_items == 'B':
            return 'task_b'
        elif selected_items == 'C':
            return 'task_c'

    with TaskGroup(group_id="First_group", tooltip="첫번째 그룹입니다.") as group_1:
        task_a = BashOperator(
            task_id='task_a',
            bash_command='echo upstream1'
        )
        @task(task_id='task_b')
        def task_b():
            print('정상처리')

        @task(task_id='task_c')
        def task_c():
            print('정상처리')


        [task_a, task_b(), task_c()]

    with TaskGroup(group_id="Second_group", tooltip="두번째 그룹입니다.") as group_2:
        @task(task_id='task_E', trigger_rule='none_skipped')
        def task_E():
            print('정상처리')

        @task(task_id='task_F', trigger_rule='none_skipped')
        def task_F():
            print('정상처리')

        task_E() >> task_F()

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상처리')


    random_branch() >> group_1 >> group_2 >> task_d()