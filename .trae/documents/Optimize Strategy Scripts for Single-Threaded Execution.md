I will optimize the stock selection strategy scripts in the database to use single-threaded execution and prevent rate limiting.

**Plan:**

1.  **Prepare Modified Scripts**:
    *   Create a temporary directory `backend/scripts/db_update` to store the modified scripts.
    *   **Script 1: "🔥 追热度：板块资金选股" (ID 6)**:
        *   Remove `concurrent.futures` and `ThreadPoolExecutor`.
        *   Replace the parallel execution loop with a standard sequential `for` loop.
        *   Add `time.sleep(0.5)` between each stock data fetch to respect API rate limits.
    *   **Script 2: "洗盘拉升突破" (ID 7)**:
        *   Remove `concurrent.futures`, `threading`, and lock mechanisms.
        *   Replace the thread pool with a sequential loop.
        *   Add `time.sleep(0.5)` between stock analysis steps.
    *   **Script 3: "山谷狙击选股" (ID 5)**:
        *   Although already single-threaded, I will inject `time.sleep(0.3)` into the main scanning loop to prevent hitting API limits during the 500-stock scan.

2.  **Apply Updates to Database**:
    *   Create a Python script `backend/scripts/db_update/apply_update.py`.
    *   This script will read the modified files and update the `stock_screeners` table in `backend/stock_watch.db` for the corresponding IDs (5, 6, 7).

3.  **Execute and Verify**:
    *   Run the update script.
    *   Verify the database has been updated successfully.
    *   Clean up the temporary directory (optional, or keep for reference).

This approach ensures all strategies run safely without triggering anti-scraping measures from data providers.