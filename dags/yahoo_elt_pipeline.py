from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import yfinance as yf
import pandas as pd


default_args = {
    'owner': 'mbahraoui',
    'retries':5,
    'retry_delay':timedelta(minutes=5)
}

def extract_data():
    symbols = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA", "NVDA", "FB", "NFLX", "PYPL", "INTC"]
    indices = ["^GSPC", "^DJI", "^IXIC", "^RUT"]

    days_back = 7

    stock_output_directory = "../data/stock_data"  
    index_output_directory = "../data/index_data"  


    end_date = datetime.datetime.today().date()

    start_date = end_date - datetime.timedelta(days=days_back)

    stock_data_df = pd.DataFrame(columns=["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    index_data_df = pd.DataFrame(columns=["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])

    for symbol in symbols:
        data = yf.download(symbol, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data["Symbol"] = symbol
        data = data[["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        stock_data_df = stock_data_df.append(data, ignore_index=True)

    for symbol in indices:
        data = yf.download(symbol, start=start_date, end=end_date)
        data.reset_index(inplace=True)
        data["Symbol"] = symbol
        data = data[["Symbol", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]
        index_data_df = index_data_df.append(data, ignore_index=True)

    stock_file_name = f"{stock_output_directory}/stock_data_{end_date}.csv"
    stock_data_df.to_csv(stock_file_name, index=False)

    index_file_name = f"{index_output_directory}/index_data_{end_date}.csv"
    index_data_df.to_csv(index_file_name, index=False)


# Define the DAG
with DAG(
    dag_id='yahoo_finance_elt_pipeline',
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

