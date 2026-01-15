# Add "Pin" (置顶) Feature to Stock Watch List

## Backend Changes

1.  **Database Model (`backend/models.py`)**:
    *   Add `is_pinned` column (Boolean, default False) to the `Stock` model.

2.  **Schemas (`backend/schemas.py`)**:
    *   Update `StockBase` to include `is_pinned: bool = False`.
    *   Update `StockUpdate` to include `is_pinned: Optional[bool] = None`.

3.  **Database Migration (`backend/main.py`)**:
    *   Update `ensure_db_schema` function to automatically add the `is_pinned` column to the `stocks` table if it doesn't exist.

4.  **API Router (`backend/routers/stocks.py`)**:
    *   Update `read_stocks` endpoint to order results by `is_pinned` (descending) and then `id` (ascending).

## Frontend Changes

1.  **Type Definitions (`frontend/src/types.ts`)**:
    *   Add `is_pinned?: boolean` to the `Stock` interface.

2.  **UI Component (`frontend/src/components/StockTable.tsx`)**:
    *   Import `PushpinOutlined` and `PushpinFilled` icons.
    *   Add a `togglePin` function to handle the API call for pinning/unpinning.
    *   Add a "Pin" column to the table (as the first column).
    *   Ensure the displayed list respects the sort order (which should be handled by the backend, but good to ensure frontend state consistency).
