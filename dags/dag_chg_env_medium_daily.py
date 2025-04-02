from airflow import DAG
import datetime
import pendulum
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

def skip_on_first_two_days():
    today = datetime.datetime.now(pendulum.timezone("Asia/Seoul"))
    if today.day in [1, 2]:
        print("Skipping DAG run on 1st or 2nd of the month.")
        return False
    return True

with DAG(
    dag_id="dag_chg_env_medium_daily",
    schedule='20 5 * * *',
    start_date=pendulum.datetime(2025, 3, 19, tz="Asia/Seoul"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["현대홈쇼핑","config", "환경"]
) as dag:
    var_value = Variable.get("chg_env_class_to_medium_cli")

    skip_check = ShortCircuitOperator(
        task_id="check_if_should_run",
        python_callable=skip_on_first_two_days
    )

    bash_var_1 = BashOperator(
        task_id="bash_var_1",
        bash_command=f"echo variable: {var_value}"
    )

    bash_var_2 = BashOperator(
        task_id="bash_var_2",
        bash_command=var_value
    )

    skip_check >> bash_var_1 >> bash_var_2
