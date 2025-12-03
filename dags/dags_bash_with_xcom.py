from airflow import DAG
import pendulum
from common.common_func import regist
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import task

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo START && "
                "echo XCOM_PUSHED "
                "{{ ti.xcom_push(key='bash_pushed', value='first_bash_message')}} &&"
                "echo COMPLETE"
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            "PUSHED_VALUE": "{{ ti.xcom_pull(key='bash_pushed') }}",
            "RETURN_VALUE": "{{ ti.xcom_pull(task_ids='bash_push') }}"
        },
        bash_command="echo START && "
            "echo PUSHED_VALUE: $PUSHED_VALUE && echo RETURN_VALUE: $RETURN_VALUE "
    )

    bash_push >> bash_pull