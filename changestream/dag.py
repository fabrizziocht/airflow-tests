from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

args = {
    'owner': 'fabri'
}

with DAG(
    start_date=datetime(2024, 1, 1),
    schedule_interval=None
) as dag:
    submit_job = SparkSubmitOperator(
        task_id='submit_job',
        application='./dags/repo/changestream/mongo_job.py',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False
    )

submit_job
