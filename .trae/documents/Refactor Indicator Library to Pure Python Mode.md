Refactor "Indicator Library" to support only "Pure Python Script" mode and migrate existing data.

## 1. Migration of Existing Data
We identified that one indicator "中短线-震荡与反转 (KDJ/WR/BIAS)" uses the legacy "AkShare + Parameters" mode but also contains a full Python script. The current script is likely failing or behaving incorrectly because it tries to use `ak` (AkShare) which is not available in the legacy post-process scope. Migrating it to "Pure Python Script" mode will resolve this issue as the script mode provides the `ak` object.

- **Action**: Create and run a migration script `scripts/migrate_indicators_to_pure_python.py`.
- **Logic**: 
    - Query all indicators with `akshare_api` set.
    - For "中短线-震荡与反转 (KDJ/WR/BIAS)", clear `akshare_api`, `params_json`, and `post_process_json`, leaving only `python_code`.
    - Verify no other indicators need complex migration (based on our analysis, only this one was active with this mode).

## 2. Code Refactoring

### Backend Refactoring
- **`backend/services/data_fetcher.py`**:
    - Remove `_apply_post_process` and `_resolve_params` methods.
    - Remove legacy logic from `fetch` method.
    - Rename `_execute_pure_script` to `execute_script` and make it the primary execution method.
    - Ensure `execute_script` injects necessary context (`ak`, `pd`, `np`, etc.).

- **`backend/routers/indicators.py`**:
    - Remove `_validate_indicator_payload` checks for `akshare_api`.
    - Update `create_indicator` and `update_indicator` to ignore `akshare_api`, `params_json`, `post_process_json`.
    - Enforce `python_code` as a required field.
    - Update `test_indicator` to use the simplified `data_fetcher`.

- **`backend/models.py`**:
    - The columns `akshare_api`, `params_json`, `post_process_json` will remain in the database schema (to avoid data loss/schema migration complexity) but will be treated as deprecated/unused.

## 3. Verification
- Run the migration script.
- Verify "中短线-震荡与反转 (KDJ/WR/BIAS)" works correctly using the "Test Indicator" feature (simulated via script).
- Verify that creating a new indicator with only Python code works.
