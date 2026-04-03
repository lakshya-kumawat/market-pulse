from pyspark.sql.types import DateType, LongType, StructType, StructField, DoubleType


OHLCV_SCHEMA = StructType([
    StructField("Date", DateType(), True),
    StructField("Open", DoubleType(), True),
    StructField("High", DoubleType(), True),
    StructField("Low", DoubleType(), True),
    StructField("Close", DoubleType(), True),
    StructField("Volume", LongType(), True),
    StructField("Dividends", DoubleType(), True),
    StructField("Stock Splits", DoubleType(), True)
])