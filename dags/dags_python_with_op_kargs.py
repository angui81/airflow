from airflow import DAG
import pendulum
from common.common_func import regist2
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_with_op_kargs",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    py_kargs_t1 = PythonOperator( # task id를 통일하는게 찾기 좋음...
        task_id="py_kargs_t1",
        python_callable=regist2,
        op_args=['전영기', '남', 'kr', 'seoul'],
        op_kwargs={ 'email': 'lucidcoder81@gmail.com', 'phone': '010-2222-3333' }
    )
    
    py_kargs_t1 # task들의 실행 순서를 선언한다