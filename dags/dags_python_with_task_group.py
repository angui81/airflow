from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task, task_group, TaskGroup

with DAG(dag_id="dags_python_with_task_group", schedule="30 6 * * *", start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), catchup=False) as dag:  # tz를 한국 시간에 맞게 설정

    def inner_func(**kwargs):
        msg = kwargs.get('msg') or ''
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        '''task_group 데커레이터를 이용한 첫 번째 그룹입니다.'''

    @task(task_id='inner_function1')
    def inner_func1(**kwargs):
        print('첫 번째 TaskGroup 내 첫 번째 task 입니다.')

    inner_function2 = PythonOperator(task_id='inner_func2', python_callable=inner_func, op_kwargs={'msg': '첫 번째 TaskGroup내 두 번째 task 입니다'})

    inner_func1() >> inner_function2

    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2:
        '''여기에 적은 docstring은 표시되지 않습니다'''

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번쨰 TaskGroup 내 첫 번쨰 task 입니다.')

        inner_function2 = PythonOperator(task_id='inner_function2', python_callable=inner_func, op_kwargs={'msg': '두번쨰 TaskGroup 내 두번째 Task 입니다.'})
