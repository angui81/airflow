from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",
    start_date=pendulum.datetime(2025, 12, 2, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False,
) as dag:
     # START_DATE : 전월 말일, END_DATE : 1일전
    bash_task_1 = BashOperator( # task id를 통일하는게 찾기 좋음...
        task_id="bash_task_1",
        env={
            'data_interval_start': '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
            'data_interval_end': '{{ data_interval_end.in_timezone("Asia/Seoul") | ds }}',
            'START_DATE': '{{ data_interval_start.in_timezone("Asia/Seoul").replace(day=1) - macros.dateutil.relativedelta.relativedelta(days=1)) | ds }}',
            'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds  }}'
        },
        bash_command='echo "*** data_interval_start: $data_interval_start" && echo "*** data_interval_end: $data_interval_end" && echo "*** START_DATE: $START_DATE" && echo "*** END_DATE: $END_DATE"',
    )

    bash_task_1