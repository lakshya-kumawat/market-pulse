from pyspark.sql import SparkSession

def get_spark_session(appname="Stock Analytics Platform"):
    spark = SparkSession.builder.appName(appname).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark