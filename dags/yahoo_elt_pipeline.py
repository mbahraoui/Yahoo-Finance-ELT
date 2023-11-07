from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3
from dotenv import load_dotenv
import os


default_args = {
    'owner': 'mbahraoui',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def check_s3_object_existence(bucket_name, object_key):
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        return True
    except Exception as e:
        return False
        

def extract_data():
    symbols = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "NVDA", "FB", "NFLX", "PYPL", "INTC"]
    indices = ["^GSPC", "^DJI", "^IXIC", "^RUT"]

    days_back = 7

    end_date = datetime.today().date()

    start_date = end_date - timedelta(days=days_back)

    stock_data_df = pd.DataFrame(columns=["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    index_data_df = pd.DataFrame(columns=["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    for symbol in symbols:
        data = yf.download(symbol, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data["Symbol"] = symbol
        data = data[["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        stock_data_df = pd.concat([stock_data_df, data], ignore_index=True)

    for symbol in indices:
        data = yf.download(symbol, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data["Symbol"] = symbol
        data = data[["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        index_data_df = pd.concat([index_data_df, data], ignore_index=True)

    stock_file_name = f"/tmp/stock_data_{end_date}.csv"
    stock_data_df.to_csv(stock_file_name, index=False)

    index_file_name = f"/tmp/index_data_{end_date}.csv"
    index_data_df.to_csv(index_file_name, index=False)

def load_to_s3():
    aws_access_key_id = os.environ.get("ACCESS_KEY")
    aws_secret_access_key = os.environ.get("SECRET")
    s3_bucket_name = 'yahoo-finance-data'
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)



    # object_exists = check_s3_object_existence(s3_bucket_name, 'company_info.csv')
    # if not (object_exists):
    #     s3.upload_file('./company_info.csv', s3_bucket_name, 'company_info.csv')

    # object_exists = check_s3_object_existence(s3_bucket_name, 'indices_info.csv')
    # if not (object_exists):
    #     s3.upload_file('./indices_info.csv', s3_bucket_name, 'indices_info.csv')

    end_date = datetime.today().date()
    s3.upload_file(f"/tmp/stock_data_{end_date}.csv", s3_bucket_name, f"stock_data/stock_data_{end_date}.csv")
    s3.upload_file(f"/tmp/index_data_{end_date}.csv", s3_bucket_name, f"index_data/index_data_{end_date}.csv")




with DAG(
    dag_id='yahoo_finance_elt_pipeline_v1',
    default_args=default_args,
    description='A DAG for the purpose of extracting data from Yahoo Finance, then storing it in AWS S3, and finally transferring it to AWS Redshift.',
    schedule_interval='@weekly',  
    start_date=datetime(2023, 11, 1),  
    catchup=False,  
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data,
    )

    load_to_s3_task = PythonOperator(
        task_id="load_to_s3_task",
        python_callable=load_to_s3
    )

    extract_data_task >> load_to_s3_task
