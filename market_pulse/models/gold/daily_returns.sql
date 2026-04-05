{{ config(materialized='table') }}

-- First create previous day's closing price for each stock
WITH previous_day_close AS (
    SELECT
        Symbol,
        Date,
        Close,
        LAG(Close, 1) OVER (PARTITION BY Symbol ORDER BY Date) AS Previous_Close
    FROM
        {{ source('silver', 'ohlcv') }}   
)

-- Now calculate daily returns and log returns for each stock
SELECT
    Symbol,
    Date,
    Close,
    Previous_Close,
    SAFE_DIVIDE(Close - Previous_Close, Previous_Close) AS Daily_Return,
    LN(SAFE_DIVIDE(Close, Previous_Close)) AS Log_Return
FROM
    previous_day_close
WHERE
    Previous_Close IS NOT NULL