I will replace all `akshare` dependencies with `tushare` across the entire codebase and database. This is a comprehensive refactoring that involves modifying core services, migration of stored scripts in the database, and updating standalone scripts.

### 1. Dependencies & Configuration

* **Update** **`requirements.txt`**: Remove `akshare`, add `tushare`.

* **Create** **`backend/tushare_client.py`**: Initialize the global Tushare Pro client with the provided token (`4501928450004005131`) and custom endpoint (`http://5k1a.xiximiao.com/dataapi`).

### 2. Core Service Refactoring

* **`backend/services/data_fetcher.py`**:

  * Replace `akshare` import with `tushare_client`.

  * Update `fetch` method to execute Tushare APIs (supporting both `pro` interface and legacy `ts` interface).

  * Inject `ts` and `pro` into the script execution scope instead of `ak`.

* **`backend/services/monitor_service.py`**:

  * Replace `ak.tool_trade_date_hist_sina` with Tushare's trade calendar API (`pro.trade_cal`).

  * Update script execution context to provide `ts` and `pro`.

* **`backend/services/screener_service.py`** **&** **`research_service.py`**:

  * Update script execution context to provide `ts` and `pro`.

### 3. Database Script Migration (Crucial)

I will write and execute a migration script to rewrite the Python code stored in the `stock_screeners` and `research_scripts` tables.

* **"山谷狙击" (Valley Sniper) Strategies**:

  * Rewrite data fetching to use `pro.daily` (History) and `ts.get_realtime_quotes` (Realtime).

  * **Logic Update**: Refactor the strategy logic to use Tushare's English field names (e.g., `close`, `vol`) instead of AKShare's Chinese keys (`收盘`, `成交量`), ensuring a "clean" migration without adapters.

  * **Unit Handling**: Ensure volume/amount units match Tushare's specifications (Vol in hands, Amount in 1000s).

* **"大盘全景看板" (Market Dashboard)**:

  * Rewrite to use `pro.index_daily`, `pro.moneyflow_hsgt` (Northbound funds), and `pro.index_classify` (Sectors).

* **Other Scripts**: Update "PB Check" and others to use `pro.stock_basic`.

### 4. Standalone Script Updates

* **`backend/scripts/*.py`**: Scan and rewrite all scripts using `akshare`.

  * `check_data.py`, `verify_akshare_data.py` (rename to `verify_tushare_data.py`), and strategy backtest scripts.

* **Frontend Templates**: Update `RuleLibrary.tsx` to provide Tushare code templates.

### 5. Verification

* Create `verify_tushare_migration.py` to test:

  * Connectivity with the custom token/URL.

  * Basic data fetching (Daily, Realtime).

  * Execution of a migrated strategy script.

