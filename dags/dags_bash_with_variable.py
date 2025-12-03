from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id="dags_bash_with_variable",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:
    var_value = Variable.get('sample_key')

    bash_var_1 = BashOperator(
        task_id='bash_var_1',
        bash_command=f"echo variable : {var_value}"
    )

    bash_var_2 = BashOperator(
        task_id='bash_var_2',
        bash_command="echo variable : {{ var.value.sample_key }}"
    )

    bash_var_1 >> bash_var_2