I have updated the plan to include the provided token.

### 1. Dependency Management
- Update `backend/requirements.txt` to include `tushare`.
- Install `tushare` in the environment.

### 2. Create Global Tushare Client
- Create `backend/utils/tushare_client.py`.
- Implement the initialization logic with the provided token:
  ```python
  import tushare as ts
  
  try:
      pro = ts.pro_api('此处不用改')
      # Use the provided token
      pro._DataApi__token = '4501928450004005131'
      pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
  except Exception as e:
      print(f"Warning: Tushare initialization failed: {e}")
      pro = None
  ```

### 3. Inject into Modules
I will modify the following services to inject `ts` (module) and `pro` (api instance) into the script execution scope, making them available globally in your scripts just like `ak`.

- **Stock Selection (选股)**: `backend/services/screener_service.py`
- **Rule Library (规则库)**: `backend/services/monitor_service.py`
- **Data Lab (数据实验室)**: `backend/services/research_service.py`
- **Indicator Library (指标库)**: `backend/services/data_fetcher.py`
