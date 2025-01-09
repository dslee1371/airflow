from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pendulum

def print_hello_workflow(**kwargs):
    print("Hello workflow deploy test")
    print("Hello workflow deploy test!!!!!!!")

def print_hello_world(**kwargs):
    dag_run_time = kwargs["logical_date"].strftime("%Y-%m-%d")
    print("dag_run_time:", dag_run_time)
    print("Hello World!!!")

default_args = {
    "owner": "test",
    "start_date": pendulum.datetime(2024, 11, 13, tz="UTC"),
}

with DAG(
    dag_id="hello_world",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start_task")

    hello_world = PythonOperator(
        task_id="print_hello_world",
        python_callable=print_hello_world,
        provide_context=True,
    )

    hello_workflow = PythonOperator(
        task_id="print_hello_workflow",
        python_callable=print_hello_workflow,
        provide_context=True,
    )

    end = EmptyOperator(task_id="end_task")

    start >> hello_world >> hello_workflow >> end
