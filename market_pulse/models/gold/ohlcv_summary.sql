{{ config(materialized='table') }}

WITH latest_date_table AS (
    SELECT 
        Symbol,
        max(Date) as Latest_Date
    FROM 
        {{ source('silver', 'ohlcv') }}
    GROUP BY Symbol
),

base AS (
    SELECT
        a.Symbol,
        a.Date,
        a.Close,
        DATE_SUB(b.Latest_Date, INTERVAL 1 WEEK) as cutoff_1w,
        DATE_SUB(b.Latest_Date, INTERVAL 1 MONTH) as cutoff_1m,
        DATE_SUB(b.Latest_Date, INTERVAL 3 MONTH) as cutoff_3m,
        DATE_SUB(b.Latest_Date, INTERVAL 6 MONTH) as cutoff_6m,
        DATE_SUB(b.Latest_Date, INTERVAL 1 YEAR) as cutoff_1y,
        DATE_SUB(b.Latest_Date, INTERVAL 5 YEAR) as cutoff_5y
    FROM 
        {{ source('silver', 'ohlcv') }} as a
    LEFT JOIN
        latest_date_table as b
    ON
        a.Symbol = b.Symbol
),

-- Returns calculated using calendar period cutoffs (DATE_SUB).
-- May differ slightly from platforms using exact trading day offsets.
ranked AS (
    SELECT
        Symbol,
        Close,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol 
            ORDER BY CASE WHEN Date<=cutoff_1w THEN Date END DESC
            ) AS rn_1w,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol 
            ORDER BY CASE WHEN Date<=cutoff_1m THEN Date END DESC
            ) AS rn_1m,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol 
            ORDER BY CASE WHEN Date<=cutoff_3m THEN Date END DESC
            ) AS rn_3m,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol 
            ORDER BY CASE WHEN Date<=cutoff_6m THEN Date END DESC
            ) AS rn_6m,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol 
            ORDER BY CASE WHEN Date<=cutoff_1y THEN Date END DESC
            ) AS rn_1y,
        ROW_NUMBER() OVER (
            PARTITION BY Symbol
            ORDER BY CASE WHEN Date<=cutoff_5y THEN Date END DESC
            ) AS rn_5y
    FROM
        base
),

return_period AS (
    SELECT
        Symbol,
        MAX(CASE WHEN rn_1w=1 THEN Close END) AS price_1w,
        MAX(CASE WHEN rn_1m=1 THEN Close END) AS price_1m,
        MAX(CASE WHEN rn_3m=1 THEN Close END) AS price_3m,
        MAX(CASE WHEN rn_6m=1 THEN Close END) AS price_6m,
        MAX(CASE WHEN rn_1y=1 THEN Close END) AS price_1y,
        MAX(CASE WHEN rn_5y=1 THEN Close END) AS price_5y
    FROM 
        ranked
    GROUP BY
        Symbol
),

volatility_calc AS (
    SELECT 
        Symbol,
        Date,
        Daily_Return,
        STDDEV(Log_Return) OVER (
            PARTITION BY Symbol
            ORDER BY Date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )*SQRT(252) AS Volatility
    FROM
        {{ ref('daily_returns') }}
),

volatility_table AS (
    SELECT
        a.Symbol,
        b.Volatility,
        b.Daily_Return,
        a.Latest_Date
    FROM 
        latest_date_table AS a
    LEFT JOIN
        volatility_calc AS b
    ON
        a.Symbol=b.Symbol AND a.Latest_Date = b.Date
),

latest_data AS (
    SELECT
        a.Symbol,
        b.Latest_Date,
        a.High as Latest_High,
        a.Low as Latest_Low,
        a.Open as Latest_Open,
        a.Close as Latest_Close,
        a.Volume as Latest_Volume
    FROM
        {{ source('silver', 'ohlcv') }} as a
    INNER JOIN
        latest_date_table as b
    ON
        a.Symbol = b.Symbol AND a.Date = b.Latest_Date
),

high_low_date AS (
    SELECT
        a.Symbol,
        a.Date,
        a.Close
    FROM
        {{ source('silver', 'ohlcv') }} AS a
    JOIN
        latest_date_table AS b
    ON 
        a.Symbol=b.Symbol
    WHERE
        a.Date>=DATE_SUB(b.Latest_Date, INTERVAL 1 YEAR)
),

high_low_calc AS (
    SELECT
        Symbol,
        MAX(Close) AS High_52W,
        MIN(Close) AS Low_52W
    FROM
        high_low_date
    GROUP BY
        Symbol
)

SELECT
    a.Symbol,
    c.Latest_Date,
    c.Latest_High,
    c.Latest_Low,
    c.Latest_Open,
    c.Latest_Close,
    c.Latest_Volume,
    b.Daily_Return AS return_1d,
    SAFE_DIVIDE(c.Latest_Close - a.price_1w, a.price_1w) AS return_1w,
    SAFE_DIVIDE(c.Latest_Close - a.price_1m, a.price_1m) AS return_1m,
    SAFE_DIVIDE(c.Latest_Close - a.price_3m, a.price_3m) AS return_3m,
    SAFE_DIVIDE(c.Latest_Close - a.price_6m, a.price_6m) AS return_6m,
    SAFE_DIVIDE(c.Latest_Close - a.price_1y, a.price_1y) AS return_1y,
    SAFE_DIVIDE(c.Latest_Close - a.price_5y, a.price_5y) AS return_5y,
    d.High_52W,
    d.Low_52W,
    b.Volatility
FROM
    return_period as a
LEFT JOIN
    volatility_table as b
ON
    a.Symbol = b.Symbol
LEFT JOIN
    latest_data as c
ON
    a.Symbol = c.Symbol
LEFT JOIN
    high_low_calc as d
ON
    a.Symbol = d.Symbol