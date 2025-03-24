
from airflow import DAG
import datetime
import pendulum
from airflow.models import Variable
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dag_chg_env_medium_daily",
    schedule='20 5 * * *',
    start_date=pendulum.datetime(2025, 3, 19, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["현대홈쇼핑","config", "환경"]
) as dag:
    var_value = Variable.get("chg_env_class_to_medium_cli")

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