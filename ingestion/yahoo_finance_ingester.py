import yfinance as yf
import json
import os
import logging
import yaml
from dotenv import load_dotenv


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def ingest_ohlcv_data(stocks_list, period, base_path, date):
    for stocks in stocks_list:
        stock_data = yf.Ticker(stocks)
        data =  stock_data.history(start="2000-01-01", period = period)

        data = data.reset_index()

        data["Date"] = data["Date"].dt.strftime("%Y-%m-%d")
        
        dir_path = f"{base_path}/ohlcv/date={date}/symbol={stocks.split('.')[0]}"
        os.makedirs(dir_path, exist_ok=True)
        with open(f"{dir_path}/raw.json", "w") as f:
            json.dump(data.to_dict(orient='records'), f, indent=4)
        logger.info(f"Data for {stocks} ingested successfully for date {date}")
        
if __name__ == "__main__":
    from datetime import date as dt
    load_dotenv()
    BRONZE_PATH = os.getenv("BRONZE_PATH")
    with open("config/stocks.yaml", "r") as f:
        config = yaml.safe_load(f)
    ingest_ohlcv_data(config["stocks"], "max", BRONZE_PATH, dt.today().strftime("%Y-%m-%d"))