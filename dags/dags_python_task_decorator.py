from airflow import DAG
import pendulum
from airflow.decorators import task;

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2025, 3, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False, # 시간 소급적용 여부 ex) 오늘이 2025, 3, 1이면 지정된 start_date(2025-01-01) 부터 오늘 전까지 다 돌릴것인지 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 시간 설정
    tags=["test", "test2"],
    params={"example_key": "example_value"}, # task에 넘겨줄 parameter
) as dag: # task들을 선언해서 사용한다
    
    @task(task_id="python_task_1")
    def print_context(some_input):
        print(some_input)
    
    run_this = print_context('task_decorator 실행!!!!!')