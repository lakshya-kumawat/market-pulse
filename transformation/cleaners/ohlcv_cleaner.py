from pyspark.sql.functions import lit, round

def clean_ohlcv(df, symbol):
    df = df.select("Date", "Open", "High", "Low", "Close", "Volume")
    df = df.withColumn("Symbol", lit(symbol))
    df = df.withColumn("Open", round(df["Open"], 2))
    df = df.withColumn("High", round(df["High"], 2))
    df = df.withColumn("Low", round(df["Low"], 2))
    df = df.withColumn("Close", round(df["Close"], 2))
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    return df