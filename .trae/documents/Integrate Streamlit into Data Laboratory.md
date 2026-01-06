I will integrate Streamlit into the Data Laboratory (Research) function, allowing you to write and run Streamlit apps directly from the interface.

### Plan
1.  **Update Dependencies**:
    *   Add `streamlit` and `watchdog` to `backend/requirements.txt`.

2.  **Backend Implementation**:
    *   Modify `backend/services/research_service.py`:
        *   Implement `launch_streamlit_script(script_content)` to save the script to a file (e.g., `backend/research_app.py`) and manage a background Streamlit process.
        *   Ensure the Streamlit process restarts or reloads when the script changes.
    *   Modify `backend/routers/research.py`:
        *   Add a new endpoint `POST /research/run_streamlit` that triggers the Streamlit launch and returns the local URL (e.g., `http://localhost:8501`).

3.  **Frontend Implementation**:
    *   Update `frontend/src/api.ts`: Add `runResearchStreamlit` API call.
    *   Update `frontend/src/components/ResearchPage.tsx`:
        *   Add a "Run with Streamlit" button in the toolbar.
        *   When clicked, call the API and open the Streamlit app in a new browser tab.

4.  **Verification**:
    *   Create a test script using Streamlit syntax (e.g., `st.title("Hello")`).
    *   Verify that clicking "Run with Streamlit" opens the app correctly.
