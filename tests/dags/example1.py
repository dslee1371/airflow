from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG 정의
with DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval='@daily',  # 매일 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # 태스크 정의
    def print_hello():
        print("Hello, Airflow!")

    task1 = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,  # 실행할 Python 함수
    )

    task1
