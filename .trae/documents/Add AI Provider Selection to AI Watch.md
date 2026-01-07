# Implementation Plan: Add AI Provider Selection to AI Watch

This update will allow users to select a specific AI provider in the "AI Watch" modal and save this preference.

## 1. Backend Updates

### Models (`backend/models.py`)
*   Update `StockAIWatchConfig` to add `ai_provider_id` column.

### Schemas (`backend/schemas.py`)
*   Update `StockAIWatchConfigBase` and `StockAIWatchConfig` to include `ai_provider_id`.

### Routes (`backend/routers/stocks.py`)
*   Update `get_ai_watch_config` to return the saved `ai_provider_id`.
*   Update `save_ai_watch_config` to save the `ai_provider_id`.
*   Update `run_ai_watch_analyze` to update the saved `ai_provider_id` when analysis runs.

### Database Migration
*   Create a script `add_ai_provider_column.py` to safely add the `ai_provider_id` column to the `stock_ai_watch_configs` table using SQL `ALTER TABLE`.

## 2. Frontend Updates

### Types (`frontend/src/types.ts`)
*   Update `StockAIWatchConfig` interface to include `ai_provider_id`.

### Component (`frontend/src/components/AIWatchModal.tsx`)
*   Fetch available AI configurations using `getAIConfigs`.
*   Add a `Select` dropdown for "AI Configuration".
*   Default the selection to the saved preference, or fall back to the stock's default AI provider.
*   Include the selected `ai_provider_id` in the analysis request.

## 3. Verification
*   Run the migration script.
*   Restart backend.
*   Open "AI Watch" modal, verify AI dropdown exists and lists available providers.
*   Select a different AI provider, run analysis, and verify it works.
*   Close and reopen modal to verify the selection is remembered.
