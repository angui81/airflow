from airflow import DAG
import random
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:
    def select_fruit():
        fruits = ['APPLE', 'BANANA', 'ORANGE']
        ran= random.randint(0, 2)
        print(fruits[ran])

    py_t1 = PythonOperator( # task id를 통일하는게 찾기 좋음...
        task_id="py_t1",
        python_callable=select_fruit
    )
    
    py_t1 # task들의 실행 순서를 선언한다