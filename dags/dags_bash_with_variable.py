
from airflow import DAG
import datetime
import pendulum
from airflow.models import Variable
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_variable",
    # 이덱은 매일 6시 30분에 시작
    schedule="10 9 * * *",
    # 덱이 언제 부터 돌지 TZ=타임존
    start_date=pendulum.datetime(2024, 7, 1, tz="Asia/Seoul"),
    # 캐치업 배치 중간에 누락된 구간을 돌릴지 말지
    # 1월 1일부터 3월 1일까지 누락된 덱을 한번에 돌아가게 된다 3월 1일에 웬만하면 false
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["인프런","bash"]
) as dag:
    var_value = Variable.get("sample_key")

    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable: {var_value}"
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        # 스케쥴러의 부하를 줄이기위해 템플릿 문법으로 전역변수를 가져오는 것을 추천함
        bash_command=var_value
    )

    bash_var_1 >> bash_var_2