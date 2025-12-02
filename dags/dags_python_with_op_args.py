from airflow import DAG
import pendulum
from common.common_func import regist
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_op_args",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    py_args_t1 = PythonOperator( # task id를 통일하는게 찾기 좋음...
        task_id="py_args_t1",
        python_callable=regist,
        op_args=['전영기', '남', '1', '2', '3']
    )
    
    py_args_t1 # task들의 실행 순서를 선언한다