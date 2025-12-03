from airflow import DAG
import pendulum
from common.common_func import regist
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_python_with_xcom_eg2",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    @task(task_id='python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return 'Success'
       
    @task(task_id='python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti=kwargs['t1']
        value1=ti.xcom_pull(task_ids='python_xcom_push_by_return')
        print('*********************************************')
        print('xcom_pull 메서드로 직접 찾은 리턴 값(value1): ' + value1)
        print('*********************************************')

    @task(task_id='python_xcom_pull_2')
    def xcom_pull_2(status, **kwargs):
        print('*********************************************')
        print('함수 입력으로 받은 값(status): ' + status)
        print('*********************************************')

    python_xcom_push_result = xcom_push_result()
    xcom_pull_2(python_xcom_push_result)
    python_xcom_push_result >> xcom_pull_1()