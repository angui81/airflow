from airflow import DAG
import pendulum
import requests
from airflow.sdk import task
from airflow.models import Variable

with DAG(dag_id="dags_sample_http_operator", schedule="30 6 * * *", start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), catchup=False) as dag:  # tz를 한국 시간에 맞게 설정

    @task(task_id='tb_cycle_station_info')
    def tb_cycle_station_info(**kwargs):
        print(kwargs)
        key = Variable.get('apikey_openapi_seoul_go_kr')

        start = Variable.get("start", 1)
        end = Variable.get("end", 5)

        print(f'*** key : { key }')
        print(f'*** start : { start }')
        print(f'*** end : { end }')
        url = f'http://openapi.seoul.go.kr:8088/{key}/json/tvCorona19VaccinestatNew/{start}/{end}/'

        print(f'*** url : { url }')
        response = requests.get(url)
        print(response.text)
        print('------------------------------------------------------------------------------------------------')
        print(f'*** status_code : { response.status_code }')
        print(response.json)

    tb_cycle_station_info()
