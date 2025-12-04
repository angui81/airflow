from airflow import DAG
import pendulum
from airflow.sdk import task
from airflow.providers.standard.operators.bash import BashOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id="dags_python_with_trigger_rule_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 4, tz="Asia/Seoul"),  # tz를 한국 시간에 맞게 설정
    catchup=False,
) as dag:

    bash_upstream1 = BashOperator(
        task_id='bash_upstream_1',
        bash_command='echo upstream1',
    )

    @task(task_id='python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!!')

    @task(task_id='python_upstream_2')
    def python_upstream_2():
        print('정상 처리')

    @task(
        task_id='python_downstream_1',
        trigger_rule='all_done',
    )
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()
