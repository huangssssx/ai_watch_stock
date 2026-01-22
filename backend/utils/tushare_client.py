import tushare as ts
import os

try:
    # Initialize with a dummy token initially if needed, but we override it below
    pro = ts.pro_api('此处不用改')
    
    # Configure with the specific token and URL provided by the user
    pro._DataApi__token = '4501928450004005131'
    pro._DataApi__http_url = 'http://5k1a.xiximiao.com/dataapi'
    
    print("Tushare client initialized successfully with custom config.")
except Exception as e:
    print(f"Warning: Tushare initialization failed: {e}")
    pro = None
