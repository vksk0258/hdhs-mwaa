from airflow import DAG
import pendulum
import datetime
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dags_base_branch_operator",
    # 이덱은 매일 6시 30분에 시작
    schedule="0 0 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런",'브랜치'],
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False
) as dag:
    class CustomPythonOperator(BaseBranchOperator):
        def choose_branch(self, context):
            import random
            print(context)
            item_list = ['A', 'B', 'C']
            selected_items = random.choice(item_list)
            if selected_items == 'A':
                return 'task_a'
            elif selected_items in ['B', 'C']:
                return ['task_b', 'task_c']

    custom_python_operator = CustomPythonOperator(task_id='python_branch_task')

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id='task_a',
        python_callable=common_func,
        op_kwargs={'selected': 'A'},
    )

    task_b = PythonOperator(
        task_id='task_b',
        python_callable=common_func,
        op_kwargs={'selected': 'B'},
    )

    task_c = PythonOperator(
        task_id='task_c',
        python_callable=common_func,
        op_kwargs={'selected': 'C'},
    )

    custom_python_operator >> [task_a, task_b, task_c]