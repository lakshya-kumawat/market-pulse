CREATE EXTERNAL TABLE `market-pulse-491904.market_pulse_silver.ohlcv`
    WITH PARTITION COLUMNS (
        Symbol STRING
    )
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://market-pulse-datalake/silver/ohlcv/*'],
        hive_partition_uri_prefix = 'gs://market-pulse-datalake/silver/ohlcv/',
        require_hive_partition_filter = false
);