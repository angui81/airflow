from airflow import DAG
import pendulum
from airflow.sdk import task
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_base_branch_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(
        2025, 12, 4, tz="Asia/Seoul"
    ),  # tz를 한국 시간에 맞게 설정
    catchup=False,
) as dag:

    class CustomBranchOperator(BaseBranchOperator):
        def choose_branch(selt, context):
            import random

            print(context)
            selected_item = random.choice(["A", "B", "C"])
            return f"task_{selected_item.lower()}"

    custom_branch_operator = CustomBranchOperator(task_id="python_branch_task")

    def common_func(**kwargs):
        print(kwargs["selected"])

    task_a = PythonOperator(
        task_id="task_a", python_callable=common_func, op_kwargs={"selected": "A"}
    )

    task_b = PythonOperator(
        task_id="task_b", python_callable=common_func, op_kwargs={"selected": "B"}
    )

    task_c = PythonOperator(
        task_id="task_c", python_callable=common_func, op_kwargs={"selected": "C"}
    )

    custom_branch_operator >> [task_a, task_b, task_c]
