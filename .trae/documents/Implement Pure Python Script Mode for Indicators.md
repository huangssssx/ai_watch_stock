I will implement the "Pure Python Script Mode" for indicators. This involves updating the data schemas to allow optional configuration and enhancing the `DataFetcher` service to execute standalone scripts.

### 1. Update Data Schemas (`backend/schemas.py`)
- Modify `IndicatorDefinitionBase` to make `akshare_api` and `params_json` optional.
- This allows the frontend to submit indicators without these fields when using "Pure Script Mode".

### 2. Enhance Data Fetcher (`backend/services/data_fetcher.py`)
- Modify the `fetch` method to check if `api_name` (akshare_api) is provided.
- **Logic**:
    - If `api_name` is present: Continue with the existing flow (fetch via akshare -> post-process).
    - If `api_name` is empty/None: Enter **Pure Script Mode**.
        - Execute `python_code` directly.
        - Inject necessary libraries (`akshare`, `pandas`, `requests`, etc.) and `context` into the script's scope.
        - Capture the output from a variable named `df` (for DataFrame) or `result` (for raw data).

### 3. Define Script Specification (for Users)
I will provide a clear specification for users to write scripts in this new mode.

**Script Rules:**
1.  **Execution Environment**: Python 3.
2.  **Built-in Libraries**: The following libraries are pre-imported and available directly:
    - `ak` (akshare)
    - `pd` (pandas)
    - `np` (numpy)
    - `requests`
    - `json`
    - `datetime`
    - `time`
3.  **Input Variables**:
    - `context` (dict): Contains context info, e.g., `{'symbol': '600519', 'name': '茅台'}`.
4.  **Output Requirement**:
    - The script **must** assign the final result to a variable named **`df`** (if it is a Pandas DataFrame) or **`result`** (if it is a list or dict).
    - The system will prioritize reading `df`. If `df` is not set or empty, it will read `result`.

### 4. Implementation Steps
1.  Modify `backend/schemas.py`.
2.  Modify `backend/services/data_fetcher.py`.
3.  Verify with a test script (I will create a temporary test to ensure it works).
