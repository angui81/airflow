from airflow import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(
    dag_id="dags_conn_test",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False, # 시간 소급적용 여부 ex) 오늘이 2025, 3, 1이면 지정된 start_date(2025-01-01) 부터 오늘 전까지 다 돌릴것인지 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 시간 설정
    tags=["conn_test", "conn_test2"],
    # params={"example_key": "example_value"}, # task에 넘겨줄 parameter
) as dag: # task들을 선언해서 사용한다
    t1 = EmptyOperator( # task id를 통일하는게 찾기 좋음...
        task_id="t1"
    )

    t2 = EmptyOperator(
        task_id="t2"
    )

    t3 = EmptyOperator(
        task_id="t3"
    )

    t4 = EmptyOperator(
        task_id="t4"
    )

    t5 = EmptyOperator(
        task_id="t5"
    )

    t6 = EmptyOperator(
        task_id="t6"
    )

    t7 = EmptyOperator(
        task_id="t7"
    )

    t8 = EmptyOperator(
        task_id="t8"
    )

    # task들의 실행 순서를 선언한다
    t1 >> [t2, t3] >> t4 
    t5 >> t4
    t7 >> t6 >> t8