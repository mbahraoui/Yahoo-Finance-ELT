from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import boto3
from dotenv import load_dotenv
import os
import psycopg2


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
    load_dotenv()
    aws_access_key_id = os.os.getenv("AWS_ACCESS_KEY")
    aws_secret_access_key = os.os.getenv("AWS_SECRET_KEY")
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


def transfer_data_to_redshift():
    load_dotenv()

    redshift_endpoint = os.getenv("REDSHIFT_ENDPOINT")
    redshift_port = os.getenv("REDSHIFT_PORT")
    redshift_database = os.getenv("REDSHIFT_DATABASE")
    redshift_user = os.getenv("REDSHIFT_USER")
    redshift_password = os.getenv("REDSHIFT_PASSWORD")
    aws_access_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret_key = os.getenv("AWS_SECRET_KEY")

    redshift_conn = psycopg2.connect(
    dbname=redshift_database,
    host=redshift_endpoint,
    port=redshift_port,
    user=redshift_user,
    password=redshift_password
    )
    redshift_cursor = redshift_conn.cursor()

    company_info_staging = """
    CREATE TABLE IF NOT EXISTS company_info_staging (
        Symbol VARCHAR(50),
        Name VARCHAR(255),
        Sector VARCHAR(255),
        Industry VARCHAR(255)
        );
    """
    redshift_cursor.execute(company_info_staging)


    stock_data_staging = """
        CREATE TABLE IF NOT EXISTS stock_data_staging (
        Symbol VARCHAR(10),
        stock_Date DATE,
        "Open" DECIMAL(20, 10),
        High DECIMAL(20, 10),
        Low DECIMAL(20, 10),
        Close DECIMAL(20, 10),
        AdjClose DECIMAL(20, 10),
        Volume BIGINT
        );
    """
    redshift_cursor.execute(stock_data_staging)


    indices_info_staging = """
        CREATE TABLE IF NOT EXISTS indices_info_staging (
            Symbol VARCHAR(50),
            Name VARCHAR(255),
            Description VARCHAR(255)
        );
    """
    redshift_cursor.execute(indices_info_staging)

    index_data_staging = """
        CREATE TABLE IF NOT EXISTS index_data_staging (
        Symbol VARCHAR(10),
        index_Date DATE,
        "Open" DECIMAL(20, 10),
        High DECIMAL(20, 10),
        Low DECIMAL(20, 10),
        Close DECIMAL(20, 10),
        AdjClose DECIMAL(20, 10),
        Volume BIGINT
        );
    """
    redshift_cursor.execute(index_data_staging)

    end_date = datetime.today().date()
    staging_table_names = ['company_info_staging', 'stock_data_staging', 'indices_info_staging', 'index_data_staging']
    s3_bucket_name = 'yahoo-finance-data'
    s3_keys= ['company_info.csv', f"stock_data/stock_data/stock_data_{end_date}.csv", 'indices_info.csv', f"index_data/index_data_{end_date}.csv"]

    for staging_table_name, s3_key in zip(staging_table_names, s3_keys):
        load_staging_data_sql = f"""
            COPY {staging_table_name}
            FROM 's3://{s3_bucket_name}/{s3_key}'
            CREDENTIALS 'aws_access_key_id={aws_access_key};aws_secret_access_key={aws_secret_key}'
            CSV
            IGNOREHEADER 1;
        """
        redshift_cursor.execute(load_staging_data_sql)


    redshift_conn.commit()
    redshift_conn.close()





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

    transfer_data_to_redshift_task = PythonOperator(
        task_id="transfer_data_to_redshift",
        python_callable=
    )

    extract_data_task >> load_to_s3_task >> transfer_data_to_redshift
