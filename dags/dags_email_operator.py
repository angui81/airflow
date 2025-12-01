from airflow import DAG
import datetime
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 11, 1, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False, # 시간 소급적용 여부 ex) 오늘이 2025, 3, 1이면 지정된 start_date(2025-01-01) 부터 오늘 전까지 다 돌릴것인지 여부
    # dagrun_timeout=datetime.timedelta(minutes=60), # timeout 시간 설정
    # tags=["conn_test", "conn_test2"],
    # params={"example_key": "example_value"}, # task에 넘겨줄 parameter
) as dag: # task들을 선언해서 사용한다
    send_email_task = EmailOperator( # task id를 통일하는게 찾기 좋음...
        task_id="send_email_task",
        to="iamyk81@naver.com",
        conn_id="conn_smtp_gmail",
        subject="Airflow 성공 메일!",
        html_content="성공 했다규!!!"
    )
    # task들의 실행 순서를 선언한다
    send_email_task 