from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False, # 시간 소급적용 여부 ex) 오늘이 2025, 3, 1이면 지정된 start_date(2025-01-01) 부터 오늘 전까지 다 돌릴것인지 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 시간 설정
    tags=["test", "test2"],
    params={"example_key": "example_value"}, # task에 넘겨줄 parameter
) as dag: # task들을 선언해서 사용한다
    bash_t1 = BashOperator( # task id를 통일하는게 찾기 좋음...
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    bash_t1 >> bash_t2 # task들의 실행 순서를 선언한다