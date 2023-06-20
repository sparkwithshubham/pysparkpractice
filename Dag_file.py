#creating my first airflow dag
from datetime import timedelta
from airflow import DAG
from airflow.opertors.python_operator import PythonOperator
from airflow.utils.date import days_ago
from datetime import datetime

default_args={
    "owners" : "airflow",
    "depends_on_past" : False,
    "start_date" : datetime(2023-6-21),
    "email" : "shubham@123.com",
    "email_on_failure" : False,
    "retries" :1,
    "retry_delay": timedelta(minutes=1),
}

dag=DAG(
    "twitter_dag",
    default_dag : default_args,
    description="My first dag with ETL process",
    schedule_interval=timedelta(days=1),
    "catchup"=False
)

run_etl=PythonOperator(
    task_id="complete_twitter_etl",
    python_callable = run_twitter_etl,
    dag=dag,
)
run_etl





