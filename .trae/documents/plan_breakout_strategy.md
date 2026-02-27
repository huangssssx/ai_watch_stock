# Plan: Implement "A-Share Breakout and Hold 3 Days" Strategy

## Goal
Implement a stock selection strategy script `backend/scripts/breakout_hold_3days.py` that identifies stocks matching the "Breakout and Hold 3 Days" (突破后站稳3日) criteria using `tushare` and `pytdx`.

## Strategy Logic
1.  **Key Levels**:
    -   High of last 60 days.
    -   60-day Moving Average (MA60).
    -   120-day Moving Average (MA120).
2.  **Breakout Condition (Day T-3)**:
    -   Close Price threshold is A-share tuned and depends on key level type:
        -   For High60 (rolling max high): Close >= Key Level * 1.005 (0.5% buffer). Using 1.5% here is often too strict because it requires a strong extension above the prior swing high.
        -   For MA60/MA120: Close >= Key Level * 1.015 (1.5% buffer), to reduce false breakouts around moving averages.
    -   Volume expansion: Volume >= 1.5 * MA20_Volume.
    -   Optional anti-pulse cap (recommended for A-share small caps): Volume <= 4.0 * MA20_Volume.
3.  **Stand Firm Condition (Day T-2, T-1, T)**:
    -   Over the 3 days after breakout, all 3 closes must satisfy Close >= Key Level * 0.99 (allow up to -1% pullback).
    -   And at least 2 out of 3 days must satisfy Close >= Key Level (must actually hold above, not only hover below).
    -   Volume check: each day Volume >= 0.5 * MA20_Volume.
    -   Optional stability check (recommended): 3-day average Volume >= 0.7 * MA20_Volume.
4.  **Data Source**:
    -   `tushare`: For filtering active stocks and potentially getting adj_factors.
    -   `pytdx`: For fetching daily bars (Open, High, Low, Close, Vol).

## Implementation Steps
1.  **Setup Script**:
    -   Create `backend/scripts/breakout_hold_3days.py`.
    -   Import `tushare_client.pro` and `pytdx_client.tdx`.
    -   Reuse helper functions for stock list and data fetching from `backend/scripts/量价形态均线_起涨前期选股.py`.

2.  **Data Fetching**:
    -   Fetch last ~150 days of daily bars to ensure enough data for MA120 and 60-day High.
    -   Filter out ST/delisted stocks using `tushare` basic info.
    -   Optional A-share hygiene filters (recommended): exclude newly listed stocks with insufficient bars (e.g., < 140 trading days) and very illiquid stocks (e.g., recent median amount too low) to avoid noisy breakouts.

3.  **Core Logic (`check_strategy`)**:
    -   Calculate MA20, MA60, MA120.
    -   Calculate Rolling Max High (60 days).
    -   Identify "Key Level" candidates (MA60, MA120, High60).
    -   Iterate backward from Today (T) to find a Breakout Day (B) at T-3.
    -   If Breakout detected at T-3 against ANY Key Level:
        -   Verify "Stand Firm" logic for T-2, T-1, T.
        -   Check Volume conditions.
    -   Keep thresholds configurable via CLI/env so you can retune quickly for different market regimes/boards (main board vs 300/688).

4.  **Output**:
    -   Generate a CSV report.
    -   Print top candidates to console.
    -   Columns: `code`, `name`, `breakout_date`, `key_level_type`, `breakout_price`, `current_price`, `vol_ratio`.

5.  **Verification**:
    -   Run the script and check if it produces results.
    -   Verify against a few known charts if possible (manual check not possible, but logical check).

## Dependencies
-   `pandas`
-   `backend.utils.tushare_client`
-   `backend.utils.pytdx_client`
