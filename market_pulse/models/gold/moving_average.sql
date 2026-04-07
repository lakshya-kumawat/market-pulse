{{ config(materialized='table') }}

SELECT
    Symbol,
    Date,
    Close,
    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS Moving_Average_7,
    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS Moving_Average_21
FROM
    {{ source('silver', 'ohlcv') }}