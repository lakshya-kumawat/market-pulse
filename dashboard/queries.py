from google.cloud import bigquery
from datetime import datetime
from dateutil.relativedelta import relativedelta

def run_query(query):
    client = bigquery.Client()
    result = client.query_and_wait(query)
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
    query = f"SELECT * FROM `market-pulse-491904.market_pulse_gold.ohlcv_summary` WHERE Symbol = '{symbol}'"
    df = run_query(query)
    return df

def get_moving_averages(symbol, period):
    start_date = get_date_range(period)
    
    query = f"SELECT * from `market-pulse-491904.market_pulse_gold.moving_average` WHERE Symbol = '{symbol}' AND Date >= '{start_date}'"
    df = run_query(query)
    return df

def get_daily_returns(symbol):
    query = f"SELECT * from `market-pulse-491904.market_pulse_gold.daily_returns` WHERE Symbol = '{symbol}' ORDER BY Date DESC LIMIT 1"
    df = run_query(query)
    return df