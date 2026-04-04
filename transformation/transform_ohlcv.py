from transformation.spark_session import get_spark_session
from transformation.schema import OHLCV_SCHEMA
from transformation.cleaners.ohlcv_cleaner import clean_ohlcv
from datetime import date
from functools import reduce


def transform_ohlcv(stocks_list, bronze_path, silver_path, date):
    
    spark = get_spark_session()
    
    dfs = []
    for stock in stocks_list:
        symbol = stock.split('.')[0]
        df = spark.read.schema(OHLCV_SCHEMA).option("multiLine", True).json(f"{bronze_path}/ohlcv/date={date}/symbol={symbol}/raw.json")
        df = clean_ohlcv(df, symbol)
        dfs.append(df)

    main_dataframe = reduce(lambda df1, df2: df1.union(df2), dfs)
    main_dataframe.write.mode("overwrite").partitionBy("Symbol").parquet(f"{silver_path}/ohlcv/")
    
if __name__ == "__main__":
    import yaml
    from dotenv import load_dotenv
    import os
    load_dotenv()
    BRONZE_PATH = os.getenv("BRONZE_PATH")
    SILVER_PATH = os.getenv("SILVER_PATH")
    with open("config/stocks.yaml", "r") as file:
        config = yaml.safe_load(file)
    stocks_list = config["stocks"]
    transform_ohlcv(stocks_list, BRONZE_PATH, SILVER_PATH, date.today().strftime("%Y-%m-%d"))