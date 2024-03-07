from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

args = {
    'owner': 'fabri'
}

with DAG('changestream_job',
         start_date=datetime(2024, 1, 1),
         schedule_interval=None
         ) as dag:
    submit_job = SparkSubmitOperator(
        task_id='submit_job',
        application='./dags/repo/changestream/mongo_job.py',
        # packages='org.mongodb.spark:mongo-spark-connector_2.12:10.2.2',
        jars='./dags/repo/changestream/mongo-spark-connector-fix-spark-3.5.0-all.jar',
        conn_id='spark_default',
        total_executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=True
    )

submit_job
