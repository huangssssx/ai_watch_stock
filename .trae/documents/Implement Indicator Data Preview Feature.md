# Implementation Plan: Indicator Data Preview Feature

This feature allows users to select indicators, fetch their raw data, and view/copy it in structured JSON format.

## 1. Backend Updates

### Service Layer (`backend/services/monitor_service.py`)
*   Implement `fetch_stock_indicators_data(stock_id, indicator_ids, db)`.
*   This function will:
    *   Fetch the stock and selected indicators.
    *   Call `data_fetcher.fetch` for each indicator.
    *   Parse the returned string (which `data_fetcher` ensures is JSON or error text) back into a Python object (list/dict) to ensure it's structured.
    *   Return a dictionary: `{ indicator_name: data_object }`.

### Routes (`backend/routers/stocks.py`)
*   Add endpoint `POST /stocks/{stock_id}/preview-indicators`.
*   Request body: `{ indicator_ids: List[int] }`.
*   Response: `{ ok: bool, data: Dict[str, Any], error: Optional[str] }`.
*   Logic: Call `fetch_stock_indicators_data` and return the result.
*   Update `save_ai_watch_config` logic (or reuse it) to save the `indicator_ids` if the user wants to persist "history". The user said "record history selection". We can reuse the existing `StockAIWatchConfig.indicator_ids` as the source of truth for "user's preferred indicators for this stock", regardless of whether they are used for AI Watch or Preview. This simplifies the UX (one set of "my indicators").

## 2. Frontend Updates

### Types (`frontend/src/types.ts`)
*   Add `IndicatorPreviewRequest` and `IndicatorPreviewResponse`.

### API (`frontend/src/api.ts`)
*   Add `previewStockIndicators(stockId, indicatorIds)`.

### Component (`frontend/src/components/IndicatorPreviewModal.tsx`)
*   Create a new modal component.
*   **UI Elements**:
    *   **Indicator Selection**: Multi-select dropdown (pre-filled from `StockAIWatchConfig`).
    *   **Fetch Button**: "获取数据" (Fetch Data).
    *   **Result Area**: A code block or text area showing the formatted JSON.
    *   **Copy Button**: Copies the content to clipboard.
*   **Logic**:
    *   On load, fetch `StockAIWatchConfig` to populate initial indicators.
    *   On "Fetch", call `previewStockIndicators`.
    *   On success, display the data `JSON.stringify(data, null, 2)`.
    *   Also save the selection back to `StockAIWatchConfig` (using `saveAIWatchConfig` API) so history is remembered.

### Stock Table (`frontend/src/components/StockTable.tsx`)
*   Add a "数据预览" (Data Preview) button (e.g., `FileSearchOutlined` or `TableOutlined`) next to "AI Watch".
*   Link it to open `IndicatorPreviewModal`.

## 3. Verification
*   Open "Data Preview" modal.
*   Select indicators (should match AI Watch selection if set).
*   Click "Fetch".
*   Verify JSON data is displayed.
*   Click "Copy" and verify clipboard content.
*   Change selection, Fetch, close, reopen AI Watch -> Verify selection is synced (since we share the config).

