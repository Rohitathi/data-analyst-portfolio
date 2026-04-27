from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import boto3
import os

default_args = {
    "owner": "rohit",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def extract(**kwargs):
    url = "https://raw.githubusercontent.com/plotly/datasets/master/sales_success.csv"
    df = pd.read_csv(url)
    os.makedirs("/tmp/airflow_data", exist_ok=True)
    df.to_parquet("/tmp/airflow_data/raw.parquet", index=False)
    print(f"Extracted {len(df)} rows")

def transform(**kwargs):
    df = pd.read_parquet("/tmp/airflow_data/raw.parquet")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df = df.dropna()
    df.to_parquet("/tmp/airflow_data/transformed.parquet", index=False)
    print(f"Transformed {len(df)} rows")

def validate(**kwargs):
    df = pd.read_parquet("/tmp/airflow_data/transformed.parquet")
    assert len(df) > 0, "Failed: empty dataset"
    assert df.isnull().sum().sum() == 0, "Failed: nulls found"
    print(f"Quality check passed: {len(df)} clean rows")

def load_to_s3(**kwargs):
    s3 = boto3.client("s3", region_name="us-east-1")
    bucket = "rohit-airflow-pipeline-2024"
    date_str = datetime.now().strftime("%Y-%m-%d")
    df = pd.read_parquet("/tmp/airflow_data/transformed.parquet")
    df.to_csv("/tmp/airflow_data/output.csv", index=False)
    s3.upload_file("/tmp/airflow_data/output.csv", bucket, f"sales/{date_str}/sales_clean.csv")
    print(f"Uploaded to s3://{bucket}/sales/{date_str}/sales_clean.csv")

def summarize(**kwargs):
    df = pd.read_parquet("/tmp/airflow_data/transformed.parquet")
    print("Total rows:", len(df))
    print("Columns:", list(df.columns))

with DAG("sales_etl_pipeline", default_args=default_args, schedule_interval="@daily",
         start_date=datetime(2024, 1, 1), catchup=False, tags=["sales", "etl"]) as dag:
    t1 = PythonOperator(task_id="extract_data", python_callable=extract)
    t2 = PythonOperator(task_id="transform_data", python_callable=transform)
    t3 = PythonOperator(task_id="validate_data", python_callable=validate)
    t4 = PythonOperator(task_id="load_to_s3", python_callable=load_to_s3)
    t5 = PythonOperator(task_id="summarize", python_callable=summarize)
    t1 >> t2 >> t3 >> t4 >> t5
