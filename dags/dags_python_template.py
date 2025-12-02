from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task;

with DAG(
    dag_id="dags_python_template",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 12, 2, tz="Asia/Seoul"), #tz를 한국 시간에 맞게 설정
    catchup=False
) as dag:

    def python_function1(start_date, end_date, **kwargs):
        print('***  start_date : ' + start_date)
        print('***  end_date : ' + end_date)

    python_t1 = PythonOperator( # task id를 통일하는게 찾기 좋음...
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date': '{{ data_interval_start | ds }}', 'end_date' : '{{ data_interval_end | ds }}'}
    )
    
    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('*** ds : ' + kwargs['ds'])
        print('*** ts : ' + kwargs['ts'])
        print('*** data_interval_start : ' + str(kwargs['data_interval_start']))
        print('*** data_interval_end : ' + str(kwargs['data_interval_end']))
        # print(f'*** task_instance : { kwargs['ti'] }')

    python_t1 >> python_function2() # task들의 실행 순서를 선언한다