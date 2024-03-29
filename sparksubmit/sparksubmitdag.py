from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG('spark_submit_job',
         start_date=datetime(2024, 1, 1),
         schedule_interval=None) as dag:
    submit_job = SparkSubmitOperator(
        task_id='submit_job',
        application='./dags/repo/sparksubmit/pyspark_script.py',
        files='./dags/repo/sparksubmit/myfile.csv',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False
    )

submit_job
