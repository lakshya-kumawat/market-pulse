from pyspark.sql.functions import lit

def clean_ohlcv(df, symbol):
    df = df.select("Date", "Open", "High", "Low", "Close", "Volume")
    df = df.withColumn("Symbol", lit(symbol))
    #later add needed checks and transformations here
    return df