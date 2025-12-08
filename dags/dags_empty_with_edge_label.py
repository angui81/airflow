from airflow import DAG
import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Lable

with DAG(dag_id="dags_empty_with_edge_label", schedule="30 6 * * *", start_date=pendulum.datetime(2025, 12, 3, tz="Asia/Seoul"), catchup=False) as dag:  # tz를 한국 시간에 맞게 설정

    empty_1 = EmptyOperator(task_id='empty_1')

    empty_2 = EmptyOperator(task_id='empty_2')

    empty_1 >> Lable('1과 2 사이') >> empty_2

    empty_3 = EmptyOperator(task_id='empty_3')

    empty_4 = EmptyOperator(task_id='empty_4')

    empty_5 = EmptyOperator(task_id='empty_5')

    empty_6 = EmptyOperator(task_id='empty_6')

    empty_2 >> Lable('Start branch') >> [empty_3, empty_4, empty_5] >> Lable('End branch') >> empty_6
