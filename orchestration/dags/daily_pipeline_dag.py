import sys
sys.path.insert(0, "/home/lakshya/stock-analytics-platform")
import os
import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
from ingestion.yahoo_finance_ingester import ingest_ohlcv_data
from transformation.transform_ohlcv import transform_ohlcv

load_dotenv()
BRONZE_PATH = os.getenv("BRONZE_PATH")
SILVER_PATH = os.getenv("SILVER_PATH")

with open("config/stocks.yaml", "r") as file:
        config = yaml.safe_load(file)
stocks_list = config["stocks"]

with DAG(
    dag_id="daily_market_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    
    ingest_task = PythonOperator(
        task_id="ingest_ohlcv_data",
        python_callable=ingest_ohlcv_data,
        op_kwargs={
            "stocks_list": stocks_list,
            "period": "1mo",
            "base_path": BRONZE_PATH,
            "date": "{{ ds }}"
        }
    )
    
    transform_task = PythonOperator(
        task_id="transform_ohlcv_data",
        python_callable=transform_ohlcv,
        op_kwargs={
            "stocks_list": stocks_list,
            "bronze_path": BRONZE_PATH,
            "silver_path": SILVER_PATH,
            "date": "{{ ds }}"
        }
    )
    
    ingest_task >> transform_task