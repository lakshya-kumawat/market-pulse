from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta

def run_query(query, job_config=None):
    client = bigquery.Client(project="market-pulse-491904")
    result = client.query_and_wait(query, job_config=job_config)
    df = result.to_dataframe()
    return df

def get_date_range(period):
    today = datetime.today()
    
    if period=="1w":
        start = today - relativedelta(weeks=1)
    elif period=="1m":
        start = today - relativedelta(months=1)
    elif period=="3m":
        start = today - relativedelta(months=3)
    elif period=="6m":
        start = today - relativedelta(months=6)
    elif period=="1y":
        start = today - relativedelta(years=1)
    elif period=="5y":
        start = today - relativedelta(years=5)
    elif period=="max":
        return '2000-01-01' # Assuming data starts from Jan 1, 2000
    else:
        raise ValueError("Invalid period. Choose from '1w', '1m', '3m', '6m', '1y', '5y', 'max'.")

    return start.strftime('%Y-%m-%d')


def get_ohlcv_summary(symbol):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol)
        ]
    )
    query = f"SELECT * FROM `market-pulse-491904.market_pulse_gold.ohlcv_summary` WHERE Symbol = @symbol"
    df = run_query(query, job_config)
    return df

def get_moving_averages(symbol, period):
    start_date = get_date_range(period)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("symbol", "STRING", symbol),
            bigquery.ScalarQueryParameter("start_date", "STRING", start_date)
        ]
    )
    query = f"SELECT * from `market-pulse-491904.market_pulse_gold.moving_average` WHERE Symbol = @symbol AND Date >= @start_date ORDER BY Date"
    df = run_query(query, job_config)
    return df