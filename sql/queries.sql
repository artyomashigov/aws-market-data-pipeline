-- ------------------------------------------------------------
-- 1. BASIC CHECKS
-- ------------------------------------------------------------

-- Preview curated prices table
SELECT *
FROM curated_prices
LIMIT 20;

-- Check available symbols and dates
SELECT DISTINCT symbol, extract_date
FROM curated_prices
ORDER BY symbol, extract_date DESC;

-- Count rows per symbol
SELECT symbol, COUNT(*) AS row_count
FROM curated_prices
GROUP BY symbol
ORDER BY symbol;


-- ------------------------------------------------------------
-- 2. PRICE ANALYSIS
-- ------------------------------------------------------------

-- Latest closing price per symbol
SELECT symbol, date, close
FROM curated_prices
WHERE date = (SELECT MAX(date) FROM curated_prices)
ORDER BY symbol;

-- Closing price history for a single symbol
SELECT symbol, date, close, volume
FROM curated_prices
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 90;

-- Highest and lowest closing price per symbol
SELECT
    symbol,
    MAX(close) AS all_time_high,
    MIN(close) AS all_time_low,
    ROUND(MAX(close) - MIN(close), 2) AS price_range
FROM curated_prices
GROUP BY symbol
ORDER BY symbol;

-- Average daily volume per symbol
SELECT
    symbol,
    ROUND(AVG(volume), 0) AS avg_daily_volume
FROM curated_prices
GROUP BY symbol
ORDER BY avg_daily_volume DESC;


-- ------------------------------------------------------------
-- 3. RAW DATA (nested JSON, before ETL)
-- ------------------------------------------------------------

SELECT
    symbol,
    extract_date,
    d.date,
    d.close,
    d.volume
FROM prices_prices
CROSS JOIN UNNEST(data) AS t(d)
LIMIT 20;
