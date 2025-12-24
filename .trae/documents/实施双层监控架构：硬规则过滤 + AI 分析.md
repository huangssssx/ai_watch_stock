# Implementation Plan: Funnel Monitoring Architecture

To implement the "Hard Rules Filter + AI Soft Analysis" architecture with flexible monitoring modes, I will introduce a reusable "Rule Script" system and update the monitoring logic.

## 1. Database Schema Changes

### New Model: `RuleScript`

Create a table to manage reusable monitoring scripts.

* `id`: Integer (PK)

* `name`: String (Unique, e.g., "Price Breakout 20-Day MA")

* `description`: String

* `code`: Text (Python code executing the logic)

* `created_at`, `updated_at`

### Update Model: `Stock`

Add fields to configure the monitoring strategy.

* `monitoring_mode`: String (Enum: `ai_only`, `script_only`, `hybrid`). Default: `ai_only`.

* `rule_script_id`: Integer (FK to `RuleScript`).

## 2. Backend Implementation

### API Extensions

* Create `backend/routers/rules.py` to provide CRUD endpoints for `RuleScripts`.

* Update `backend/main.py` to include the new router.

* Update `Stock` related schemas and endpoints to support selecting `monitoring_mode` and `rule_script_id`.

### Monitor Service Refactoring (`backend/services/monitor_service.py`)

Refactor `process_stock` to support the three modes:

1. **AI Only (Default)**: Existing logic (Fetch Indicators -> AI -> Alert).
2. **Script Only**:

   * Execute the linked `RuleScript`.

   * Script fetches its own data (via `akshare`).

   * If `triggered=True`: Send Alert (Email) directly. Skip AI.
3. **Hybrid (Funnel)**:

   * Execute the linked `RuleScript`.

   * If `triggered=False`: Stop (Log "Skipped by Rule").

   * If `triggered=True`:

     * Fetch `Stock.indicators` (context data).

     * Call AI Analysis (Pass script output as context).

     * Send Alert based on AI result.

### Script Execution Engine

Implement `_execute_rule_script(code, stock_context)`:

* Provides a sandboxed environment with `akshare`, `pandas`, `numpy`, `datetime`.

* Captures variables `triggered` (bool) and `message` (str) from the script scope.

## 3. Migration

* Provide a Python script (`migrations/add_rule_scripts.py`) to update the database schema (add tables and columns) since we are not using a migration tool like Alembic explicitly in the file list.

## 4. Verification

* Create a test `RuleScript` (e.g., "Always Trigger" and "Never Trigger").

* Run the monitor service to verify the flow for each mode.

