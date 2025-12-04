from airflow import (
    DAG,
)
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_python_with_trigger_rule_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 4, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    @task.branch
    def randonm_branch():
        import random

        selected_item = random.choice(
            [
                'A',
                'B',
                'C',
            ]
        )
        return f'task_{selected_item.lower()}'

    task_a = BashOperator(task_id='task_a', bash_command='echo upstream1')

    @task(task_id='task_b')
    def task_b():
        print('정상 처리')

    @task(task_id='task_c')
    def task_c():
        print('정상 처리')

    @task(task_id='task_d', trigger_rule='none_skipped')
    def task_d():
        print('정상 처리')

    randonm_branch() >> [task_a, task_b(), task_c()] >> task_d()
