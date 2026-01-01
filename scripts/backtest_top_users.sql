-- ============================================================================
-- POLYMARKET TOP 500 USER COPY-TRADING BACKTEST
-- ============================================================================
-- 
-- This SQL script backtests a strategy that copies trades from the top 500
-- Polymarket users (by PnL) using DuckDB.
--
-- Strategy: When a top user buys, we buy. When they sell, we sell.
-- Exit: We exit positions when the market resolves (using final price).
--
-- IMPORTANT: Due to JSON parsing issues in DuckDB, the recommended way to run
-- this backtest is using the Python runner: scripts/run_backtest.py
-- 
-- The Python runner handles JSON parsing safely and provides formatted output.
--
-- Usage:
--   python scripts/run_backtest.py           # Copy top 500 users
--   python scripts/run_backtest.py --top 100 # Copy top 100 users
--   python scripts/run_backtest.py --export  # Export results to CSV
--
-- Data Sources (Parquet files):
--   - leaderboard: Top users by PnL
--   - trades: Individual trade records
--   - gamma_markets: Market metadata with resolution info
--   - prices: Price history for P&L calculation
--
-- ============================================================================

-- Set the data directory
SET VARIABLE data_dir = 'c:\Users\User\Desktop\VibeCoding\PolyMarketScrapping\data';

-- ============================================================================
-- STEP 1: CREATE VIEWS FOR PARQUET DATA
-- ============================================================================

-- Load leaderboard data (top users by PnL)
CREATE OR REPLACE VIEW v_leaderboard AS
SELECT 
    CAST(rank AS INTEGER) as rank_num,
    proxyWallet,
    userName,
    xUsername,
    vol,
    pnl,
    dt
FROM read_parquet(getenv('data_dir') || '/leaderboard/**/*.parquet', 
                  hive_partitioning=true, union_by_name=true);

-- Load trade data
CREATE OR REPLACE VIEW v_trades AS
SELECT 
    proxyWallet,
    side,
    price,
    size,
    conditionId,
    timestamp,
    transactionHash,
    outcome,
    dt,
    -- Calculate USD value of trade
    price * size AS trade_value_usd
FROM read_parquet(getenv('data_dir') || '/trades/**/*.parquet',
                  hive_partitioning=true, union_by_name=true);

-- Load gamma markets data (has resolution info)
CREATE OR REPLACE VIEW v_markets AS
SELECT 
    id,
    conditionId,
    question,
    slug,
    category,
    description,
    active,
    closed,
    startDate,
    endDate,
    outcomes,
    outcomePrices,
    clobTokenIds,
    volumeNum,
    liquidityNum,
    dt
FROM read_parquet(getenv('data_dir') || '/gamma_markets/**/*.parquet',
                  hive_partitioning=true, union_by_name=true);

-- Load price history
CREATE OR REPLACE VIEW v_prices AS
SELECT 
    timestamp,
    token_id,
    price,
    dt
FROM read_parquet(getenv('data_dir') || '/prices/**/*.parquet',
                  hive_partitioning=true, union_by_name=true);


-- ============================================================================
-- STEP 2: IDENTIFY TOP 500 USERS BY CUMULATIVE PNL
-- ============================================================================

-- Get the top 500 users based on the latest leaderboard snapshot
CREATE OR REPLACE VIEW v_top_500_users AS
WITH latest_leaderboard AS (
    SELECT 
        proxyWallet,
        userName,
        pnl,
        vol,
        dt,
        ROW_NUMBER() OVER (PARTITION BY proxyWallet ORDER BY dt DESC) as rn
    FROM v_leaderboard
    WHERE rank_num <= 500  -- Only consider top 500 from each snapshot
)
SELECT DISTINCT
    proxyWallet,
    userName,
    pnl as latest_pnl,
    vol as total_volume
FROM latest_leaderboard
WHERE rn = 1
ORDER BY latest_pnl DESC
LIMIT 500;


-- ============================================================================
-- STEP 3: GET ALL TRADES FROM TOP 500 USERS
-- ============================================================================

CREATE OR REPLACE VIEW v_top_user_trades AS
SELECT 
    t.proxyWallet,
    u.userName,
    u.latest_pnl as user_pnl,
    t.side,
    t.price,
    t.size,
    t.conditionId,
    t.outcome,
    t.timestamp,
    t.transactionHash,
    t.trade_value_usd,
    -- Timestamp as datetime
    to_timestamp(t.timestamp) as trade_datetime
FROM v_trades t
INNER JOIN v_top_500_users u ON t.proxyWallet = u.proxyWallet;


-- ============================================================================
-- STEP 4: PARSE MARKET RESOLUTION DATA
-- ============================================================================

-- Extract resolution prices from markets (outcomePrices field)
-- Closed markets have final prices of 0 or 1 (winner takes all)
CREATE OR REPLACE VIEW v_market_resolution AS
SELECT 
    conditionId,
    question,
    category,
    closed,
    active,
    volumeNum,
    endDate,
    outcomes,
    outcomePrices,
    clobTokenIds,
    -- Parse the first and second outcome prices (JSON array format)
    TRY_CAST(json_extract_string(outcomePrices, '$[0]') AS DOUBLE) as outcome1_final_price,
    TRY_CAST(json_extract_string(outcomePrices, '$[1]') AS DOUBLE) as outcome2_final_price,
    json_extract_string(outcomes, '$[0]') as outcome1_name,
    json_extract_string(outcomes, '$[1]') as outcome2_name,
    -- Check if resolved (price is 0 or 1)
    CASE 
        WHEN closed = true AND (
            TRY_CAST(json_extract_string(outcomePrices, '$[0]') AS DOUBLE) IN (0, 1) OR
            TRY_CAST(json_extract_string(outcomePrices, '$[1]') AS DOUBLE) IN (0, 1)
        ) THEN true
        ELSE false
    END as is_resolved
FROM v_markets;


-- ============================================================================
-- STEP 5: CALCULATE BACKTEST P&L FOR COPY TRADES
-- ============================================================================

-- Main backtest calculation
-- Strategy: Copy every trade from top 500 users with a fixed position size
CREATE OR REPLACE VIEW v_backtest_trades AS
WITH trade_with_resolution AS (
    SELECT 
        t.proxyWallet,
        t.userName,
        t.side,
        t.price as entry_price,
        t.size as original_size,
        t.conditionId,
        t.outcome,
        t.trade_datetime,
        t.trade_value_usd,
        m.question,
        m.category,
        m.closed as market_closed,
        m.is_resolved,
        m.outcome1_name,
        m.outcome2_name,
        m.outcome1_final_price,
        m.outcome2_final_price,
        -- Determine final price for this outcome
        CASE 
            WHEN t.outcome = m.outcome1_name THEN m.outcome1_final_price
            WHEN t.outcome = m.outcome2_name THEN m.outcome2_final_price
            ELSE NULL
        END as final_price
    FROM v_top_user_trades t
    LEFT JOIN v_market_resolution m ON t.conditionId = m.conditionId
)
SELECT 
    *,
    -- Calculate P&L per share (assuming we copy with same size)
    CASE 
        WHEN is_resolved AND final_price IS NOT NULL THEN
            CASE 
                WHEN side = 'BUY' THEN (final_price - entry_price)
                WHEN side = 'SELL' THEN (entry_price - final_price)
                ELSE 0
            END
        ELSE NULL  -- Unresolved markets have no P&L yet
    END as pnl_per_share,
    -- Calculate total P&L (assuming we match original size)
    CASE 
        WHEN is_resolved AND final_price IS NOT NULL THEN
            CASE 
                WHEN side = 'BUY' THEN (final_price - entry_price) * original_size
                WHEN side = 'SELL' THEN (entry_price - final_price) * original_size
                ELSE 0
            END
        ELSE NULL
    END as total_pnl
FROM trade_with_resolution;


-- ============================================================================
-- STEP 6: AGGREGATE BACKTEST RESULTS
-- ============================================================================

-- Summary by user - how well does copying each user perform?
CREATE OR REPLACE VIEW v_user_backtest_summary AS
SELECT 
    proxyWallet,
    userName,
    COUNT(*) as total_trades,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_trades,
    COUNT(CASE WHEN is_resolved AND total_pnl > 0 THEN 1 END) as winning_trades,
    COUNT(CASE WHEN is_resolved AND total_pnl < 0 THEN 1 END) as losing_trades,
    SUM(CASE WHEN is_resolved THEN ABS(trade_value_usd) ELSE 0 END) as total_volume_copied,
    SUM(COALESCE(total_pnl, 0)) as copy_strategy_pnl,
    AVG(CASE WHEN is_resolved THEN total_pnl END) as avg_pnl_per_trade,
    -- Win rate
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND total_pnl > 0 THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN is_resolved THEN 1 END), 0), 2) as win_rate_pct
FROM v_backtest_trades
GROUP BY proxyWallet, userName
ORDER BY copy_strategy_pnl DESC;


-- Overall strategy summary
CREATE OR REPLACE VIEW v_overall_backtest_summary AS
SELECT 
    'Top 500 Copy Strategy' as strategy_name,
    COUNT(DISTINCT proxyWallet) as users_copied,
    COUNT(*) as total_trades,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_trades,
    COUNT(CASE WHEN is_resolved AND total_pnl > 0 THEN 1 END) as winning_trades,
    COUNT(CASE WHEN is_resolved AND total_pnl < 0 THEN 1 END) as losing_trades,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND total_pnl > 0 THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN is_resolved THEN 1 END), 0), 2) as win_rate_pct,
    SUM(CASE WHEN is_resolved THEN trade_value_usd ELSE 0 END) as total_capital_deployed,
    SUM(COALESCE(total_pnl, 0)) as total_pnl,
    AVG(CASE WHEN is_resolved THEN total_pnl END) as avg_pnl_per_trade,
    STDDEV(CASE WHEN is_resolved THEN total_pnl END) as pnl_std_dev,
    -- Sharpe-like ratio (simplified)
    AVG(CASE WHEN is_resolved THEN total_pnl END) / 
        NULLIF(STDDEV(CASE WHEN is_resolved THEN total_pnl END), 0) as pnl_to_risk_ratio
FROM v_backtest_trades;


-- ============================================================================
-- STEP 7: CATEGORY-LEVEL ANALYSIS
-- ============================================================================

CREATE OR REPLACE VIEW v_category_performance AS
SELECT 
    category,
    COUNT(*) as total_trades,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_trades,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND total_pnl > 0 THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN is_resolved THEN 1 END), 0), 2) as win_rate_pct,
    SUM(COALESCE(total_pnl, 0)) as total_pnl,
    AVG(CASE WHEN is_resolved THEN total_pnl END) as avg_pnl_per_trade
FROM v_backtest_trades
GROUP BY category
ORDER BY total_pnl DESC;


-- ============================================================================
-- STEP 8: TIME-BASED ANALYSIS (Monthly/Daily returns)
-- ============================================================================

CREATE OR REPLACE VIEW v_daily_returns AS
SELECT 
    DATE_TRUNC('day', trade_datetime) as trade_date,
    COUNT(*) as trades_count,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_count,
    SUM(COALESCE(total_pnl, 0)) as daily_pnl,
    SUM(CASE WHEN is_resolved THEN trade_value_usd ELSE 0 END) as daily_volume
FROM v_backtest_trades
GROUP BY DATE_TRUNC('day', trade_datetime)
ORDER BY trade_date;

CREATE OR REPLACE VIEW v_monthly_returns AS
SELECT 
    DATE_TRUNC('month', trade_datetime) as trade_month,
    COUNT(*) as trades_count,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_count,
    SUM(COALESCE(total_pnl, 0)) as monthly_pnl,
    SUM(CASE WHEN is_resolved THEN trade_value_usd ELSE 0 END) as monthly_volume,
    -- Cumulative P&L
    SUM(SUM(COALESCE(total_pnl, 0))) OVER (ORDER BY DATE_TRUNC('month', trade_datetime)) as cumulative_pnl
FROM v_backtest_trades
GROUP BY DATE_TRUNC('month', trade_datetime)
ORDER BY trade_month;


-- ============================================================================
-- STEP 9: FIXED POSITION SIZE BACKTEST (Alternative Strategy)
-- ============================================================================

-- Instead of copying exact sizes, use fixed $100 per trade
CREATE OR REPLACE VIEW v_fixed_size_backtest AS
WITH fixed_trades AS (
    SELECT 
        proxyWallet,
        userName,
        side,
        entry_price,
        conditionId,
        outcome,
        trade_datetime,
        question,
        category,
        is_resolved,
        final_price,
        100.0 as position_size_usd,  -- Fixed $100 per trade
        100.0 / NULLIF(entry_price, 0) as shares_bought  -- Shares we can buy
    FROM v_backtest_trades
    WHERE entry_price > 0
)
SELECT 
    *,
    -- Calculate P&L with fixed position
    CASE 
        WHEN is_resolved AND final_price IS NOT NULL THEN
            CASE 
                WHEN side = 'BUY' THEN (final_price - entry_price) * shares_bought
                WHEN side = 'SELL' THEN (entry_price - final_price) * shares_bought
                ELSE 0
            END
        ELSE NULL
    END as fixed_position_pnl
FROM fixed_trades;


-- Summary for fixed position strategy
CREATE OR REPLACE VIEW v_fixed_size_summary AS
SELECT 
    'Fixed $100 Copy Strategy' as strategy_name,
    COUNT(DISTINCT proxyWallet) as users_copied,
    COUNT(*) as total_trades,
    COUNT(CASE WHEN is_resolved THEN 1 END) as resolved_trades,
    SUM(position_size_usd) as total_capital_needed,
    SUM(COALESCE(fixed_position_pnl, 0)) as total_pnl,
    ROUND(100.0 * COUNT(CASE WHEN is_resolved AND fixed_position_pnl > 0 THEN 1 END) / 
          NULLIF(COUNT(CASE WHEN is_resolved THEN 1 END), 0), 2) as win_rate_pct,
    AVG(CASE WHEN is_resolved THEN fixed_position_pnl END) as avg_pnl_per_trade,
    -- ROI
    ROUND(100.0 * SUM(COALESCE(fixed_position_pnl, 0)) / 
          NULLIF(SUM(position_size_usd), 0), 2) as roi_pct
FROM v_fixed_size_backtest;


-- ============================================================================
-- STEP 10: TOP PERFORMER ANALYSIS
-- ============================================================================

-- Which top users are most profitable to copy?
CREATE OR REPLACE VIEW v_best_users_to_copy AS
SELECT 
    proxyWallet,
    userName,
    total_trades,
    resolved_trades,
    winning_trades,
    losing_trades,
    win_rate_pct,
    total_volume_copied,
    copy_strategy_pnl,
    avg_pnl_per_trade,
    -- Rank by profitability
    ROW_NUMBER() OVER (ORDER BY copy_strategy_pnl DESC) as profit_rank,
    -- Rank by win rate (min 10 resolved trades)
    CASE 
        WHEN resolved_trades >= 10 
        THEN ROW_NUMBER() OVER (
            ORDER BY CASE WHEN resolved_trades >= 10 THEN win_rate_pct ELSE -1 END DESC
        )
        ELSE NULL
    END as win_rate_rank
FROM v_user_backtest_summary
WHERE resolved_trades >= 5  -- Filter users with enough data
ORDER BY copy_strategy_pnl DESC
LIMIT 100;


-- ============================================================================
-- EXECUTION: RUN THE BACKTEST QUERIES
-- ============================================================================

-- Uncomment these to run the analysis:

-- 1. Show overall strategy performance
-- SELECT * FROM v_overall_backtest_summary;

-- 2. Show best users to copy
-- SELECT * FROM v_best_users_to_copy LIMIT 20;

-- 3. Show category performance
-- SELECT * FROM v_category_performance;

-- 4. Show monthly returns
-- SELECT * FROM v_monthly_returns;

-- 5. Show fixed position strategy summary
-- SELECT * FROM v_fixed_size_summary;

-- 6. Show top 500 users identified
-- SELECT * FROM v_top_500_users LIMIT 20;

-- 7. Show sample trades
-- SELECT * FROM v_backtest_trades LIMIT 100;


-- ============================================================================
-- QUICK STATS QUERY (Run this for a quick overview)
-- ============================================================================

/*
-- Run all main summaries at once:

SELECT '=== TOP 500 USERS ===' as section;
SELECT COUNT(*) as top_users_count FROM v_top_500_users;

SELECT '=== THEIR TRADES ===' as section;
SELECT 
    COUNT(*) as total_trades,
    COUNT(DISTINCT proxyWallet) as unique_traders,
    COUNT(DISTINCT conditionId) as unique_markets
FROM v_top_user_trades;

SELECT '=== OVERALL BACKTEST ===' as section;
SELECT * FROM v_overall_backtest_summary;

SELECT '=== TOP 10 USERS TO COPY ===' as section;
SELECT * FROM v_best_users_to_copy LIMIT 10;

SELECT '=== CATEGORY BREAKDOWN ===' as section;
SELECT * FROM v_category_performance;
*/
