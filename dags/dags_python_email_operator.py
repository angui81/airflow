from airflow import DAG
import pendulum
from airflow.sdk import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id="dags_python_email_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    @task(task_id='something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Success', 'Fail'])

    send_email = EmailOperator(
        task_id='send_email',
        to='iamyk81@naver.com',
        subject='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과',
        html_content='{{ ti.xcom_pull(task_ids="something_task") }} !!!'
    )
    
    some_logic() >> send_email