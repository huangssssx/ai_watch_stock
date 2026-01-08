# Add "Remark" (备注) Column to Stock Watch Table

I will implement the "Remark" feature to allow you to add plain text notes to your watched stocks.

## Backend Implementation
1.  **Database Model (`backend/models.py`)**:
    *   Add a `remark` column (Text type) to the `Stock` table.
2.  **API Schemas (`backend/schemas.py`)**:
    *   Update `StockBase`, `StockUpdate`, and `Stock` schemas to include the `remark` field.
3.  **Database Migration**:
    *   Create and run a python script `backend/scripts/add_remark_column.py` to safely add the `remark` column to your existing `stock_watch.db`.
4.  **API Logic (`backend/routers/stocks.py`)**:
    *   Ensure the `update_stock` endpoint correctly handles the `remark` field (likely handled automatically by Pydantic/SQLAlchemy, but will verify).

## Frontend Implementation
1.  **Type Definitions (`frontend/src/types.ts`)**:
    *   Add `remark?: string` to the `Stock` interface.
2.  **UI Update (`frontend/src/components/StockTable.tsx`)**:
    *   Add a new column "备注" to the table.
    *   Implement an **editable cell**:
        *   Display the remark text.
        *   Clicking the cell (or an edit icon) allows you to edit the text using a text area.
        *   Saving (e.g., on blur or Enter) updates the backend via API.

## Verification
*   I will verify that the column is added to the database.
*   I will verify that you can add, edit, and save remarks in the frontend.
