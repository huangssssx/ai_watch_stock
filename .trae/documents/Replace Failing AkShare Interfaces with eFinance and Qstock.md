I will replace the failing AkShare interfaces with `efinance` and `qstock` as requested.

**Plan:**

1. **Environment Setup**:

   * Install `efinance` and `qstock` using `pip`.

2. **API Verification (Probe)**:

   * Create a temporary script `backend/scripts/db_update/probe_alternatives.py` to call the recommended alternative functions.

   * Inspect the returned DataFrame columns to ensure they match the existing logic (or determine the necessary column renaming).

3. **Refactor Scripts**:

   * **Script 1: Chase Heat (`chase_heat_safe.py`)**:

     * Replace `ak.stock_zh_a_spot_em` with `ef.stock.get_realtime_quotes`.

     * Replace `ak.stock_zh_a_hist` with `qs.get_data` (or `ef` equivalent if `qs` fails).

     * Replace `ak.stock_board_industry_name_em` with `qs.industry_list`.

     * Replace `ak.stock_board_industry_cons_em` with `qs.industry_member`.

   * **Script 2: Wash Markup (`wash_markup_safe.py`)**:

     * Replace `ak.stock_zh_a_spot_em` with `ef.stock.get_realtime_quotes`.

     * Replace `ak.stock_zh_a_hist` with `qs.get_data`.

     * Replace `ak.stock_zh_index_spot_em` with `qs.realtime_data(market='index')`.

     * Replace `ak.stock_zh_index_daily_em` with `qs.get_data`.

   * **Script 3: Valley Sniper (`valley_sniper_safe.py`)**:

     * Replace `ak.stock_zh_a_spot_em` with `ef.stock.get_realtime_quotes`.

     * Replace `ak.stock_zh_a_hist` with `qs.get_data`.

     * Replace `ak.stock_sector_fund_flow_rank` with `qs.realtime_money_flow`.

     * Keep `ak.stock_hot_rank_em` and `ak.stock_individual_info_em` if they are still working (as per previous test), otherwise find alternatives.

4. **Verification**:

   * Run each modified script locally to ensure no "0 results" due to API errors.

5. **Database Update**:

   * Run `apply_update.py` to persist the changes to `stock_watch.db`.

