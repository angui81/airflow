from airflow import DAG
import pendulum
from common.common_func import regist
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo PUSH START "
                "{{ ti.xcom_push(key='bash_pushed', value=200)}} &&"
                "echo COMPLETE"
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti=kwargs['ti']
        status_value=ti.xcom_pull(key='bash_pushed')
        return_value=ti.xcom_pull(key='return_value')
        print("status_value : " + str(status_value));
        print("return_value : " + str(return_value));

    bash_push >> python_pull_xcom()