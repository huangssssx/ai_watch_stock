# Implementation Plan: AI Watch Feature (AI 看盘)

This feature will allow users to manually trigger an AI analysis for a specific stock with customized indicators and prompts, while preserving their preferences.

## 1. Database Safety & Migration

* **Backup**: Create a backup of `stock_watch.db` to `stock_watch.db.bak` before any changes.

* **Schema Change**: Add a new table `stock_ai_watch_configs` (mapped to `StockAIWatchConfig` model) to store user preferences per stock.

  * `stock_id`: Foreign Key to Stock.

  * `indicator_ids`: JSON list of selected indicator IDs.

  * `custom_prompt`: Text field for the custom prompt.

  * `updated_at`: Timestamp.

* **Strategy**: Using `Base.metadata.create_all()` will automatically create the new table without affecting existing data.

## 2. Backend Implementation

### Models & Schemas

* Update `backend/models.py`: Add `StockAIWatchConfig`.

* Update `backend/schemas.py`: Add Pydantic models for the config and analysis request.

### Service Layer (`backend/services/monitor_service.py`)

* Implement `analyze_stock_manual(stock_id, indicator_ids, custom_prompt, ai_provider_id)`:

  * Reuse `data_fetcher` to fetch data for the *specific* indicators selected (ignoring the stock's default configured indicators).

  * Construct the prompt using the `custom_prompt` as the analysis strategy.

  * Call `ai_service` to get the analysis result.

  * Return the structured result (JSON) + raw text.

### API Routes (`backend/routers/stocks.py`)

* `GET /stocks/{id}/ai-watch-config`: Retrieve the last used configuration.

* `POST /stocks/{id}/ai-watch-config`: Save the current configuration.

* `POST /stocks/{id}/ai-watch-analyze`: Trigger the manual analysis.

## 3. Frontend Implementation

### API Client (`frontend/src/api.ts`)

* Add methods for `getAIWatchConfig`, `saveAIWatchConfig`, and `runAIWatchAnalysis`.

### New Component (`frontend/src/components/AIWatchModal.tsx`)

* **UI Elements**:

  * **Indicator Selection**: Multi-select dropdown (pre-filled with saved history).

  * **Custom Prompt**: Text area (pre-filled with saved history).

  * **Analyze Button**: Triggers the API.

  * **Result Display**: Show the AI's analysis (Signal, Advice, Logic) and raw output.

* **Logic**:

  * On open: Fetch available indicators and load saved config for this stock.

  * On "Analyze": Save the config first, then trigger analysis.

### Stock Table (`frontend/src/components/StockTable.tsx`)

* Add an "AI Watch" button (e.g., an Eye icon or "AI 看盘" button) to the actions column.

* Link it to open the `AIWatchModal`.

## 4. Verification

* **Test 1**: Verify DB backup is created.

* **Test 2**: Verify the new table is created after restart.

* **Test 3**: Open "AI Watch" modal, select indicators/prompt, and ensure they are saved (close and reopen to check persistence).

* **Test 4**: Click "Analyze" and verify the AI returns a response based on the selected indicators.

<br />

5、you need to save last 3 analysis history 
