from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * 6#1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False, # 시간 소급적용 여부 ex) 오늘이 2025, 3, 1이면 지정된 start_date(2025-01-01) 부터 오늘 전까지 다 돌릴것인지 여부
) as dag: # task들을 선언해서 사용한다
    bash_t1 = BashOperator( # task id를 통일하는게 찾기 좋음...
        task_id="bash_t1",
        bash_command='echo "data_interval_end: {{ data_interval_end }}"' ,
    )

    bash_t2 = BashOperator( # task id를 통일하는게 찾기 좋음...
        task_id="bash_t2",
        env= {
            'START_DATE': '{{ data_interval_start | ds }}',
            'END_DATE': '{{ data_interval_end | ds }}',
        },
        bash_command='echo * START_DATE : $START_DATE && echo * END_DATE : $END_DATE'
    )

    bash_t1 >> bash_t2
 