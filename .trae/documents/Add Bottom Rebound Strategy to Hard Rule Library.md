I will add a **"Bottom Rebound Monitoring" (触底反弹监控)** strategy to your Hard Rule Script Library.

This strategy will help you identify stocks that are recovering from a decline, acting as the "signal" you requested to judge if they are "ready to rise".

### Plan:

1.  **Create Strategy Script**:
    *   Create `backend/scripts/跟踪策略/触底反弹监控.py`.
    *   **Logic**:
        *   **Trend Filter**: Ensure the stock was in a downtrend or at a low level (e.g., Price < MA60 or recent low).
        *   **Rebound Signal**:
            *   **Price Action**: Price breaks above MA5.
            *   **Momentum**: MACD Golden Cross (Diff crosses DEA upwards) or Histogram turns positive.
            *   **Oversold Recovery**: RSI was low (< 40) and is now rising.
        *   **Output**: Triggers a `BUY` signal with a message like "触底反弹：站上MA5+MACD金叉".

2.  **Register Strategy**:
    *   Create a helper script `backend/scripts/insert_rebound_rule.py` to insert this new script into the system database (`rule_scripts` table).
    *   Execute this script to make the rule available in your library.

3.  **Verification**:
    *   Confirm the rule is added successfully so you can select it in the frontend for your stocks.
