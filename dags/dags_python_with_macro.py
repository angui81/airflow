from airflow import DAG
import pendulum
from airflow.sdk import task

with DAG(
    dag_id="dags_python_with_macro",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 2, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    @task(
        task_id='task_using_macros',
        templates_dict={ 
            'start_date':'{{ (data_interval_end.in_timezone("Asia/Seoul") + macros.dateutil.relativedelta.relativedelta(months=-1, day=1)) | ds }}',
            'end_date': '{{ (data_interval_end.in_timezone("Asia/Seoul").replace(day=1) +macros.dateutil.relativedelta.relativedelta(days=-1)) | ds }}'
        }
    )
    def get_datetime_macro(**kwargs):
        templates_dict = kwargs.get('templates_dict') or {}
        if templates_dict:
            start_date = templates_dict.get('start_date') or 'no start_date'
            end_date = templates_dict.get('end_date') or 'no end_date'
            print("***** get_datetime_macro start_date : " + str(start_date))
            print("***** get_datetime_macro end_date : "+ str(end_date))

    @task(task_id='task_direct_calc')
    def get_datetine_calc(**kwargs):
        from dateutil.relativedelta import relativedelta

        date_interval_end = kwargs['data_interval_end']
        prev_month_day_first = date_interval_end.in_timezone("Asia/Seoul") + relativedelta(months=-1, day=1)
        prev_month_day_last = date_interval_end.in_timezone("Asia/Seoul").replace(day=1) + relativedelta(days=-1)

        print('*** prev_month_day_first : ' + prev_month_day_first.strftime('%Y-%m-%d'))
        print('*** prev_month_day_last : ' + prev_month_day_last.strftime('%Y-%m-%d'))
    
    get_datetime_macro() >> get_datetine_calc()