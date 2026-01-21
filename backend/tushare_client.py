
import tushare as ts
import os
import time

# Use the token provided by the user
TS_TOKEN = os.getenv("TUSHARE_TOKEN", "4501928450004005131")
TS_API_URL = os.getenv("TUSHARE_API_URL", "http://5k1a.xiximiao.com/dataapi")

try:
    ts.set_token(TS_TOKEN)
    pro = ts.pro_api()
    pro._DataApi__http_url = TS_API_URL
    print(f"Tushare Client Initialized. URL: {TS_API_URL}")
except Exception as e:
    print(f"Error initializing Tushare client: {e}")
    pro = None

def get_pro_client():
    if pro is None:
        # Retry init
        try:
            ts.set_token(TS_TOKEN)
            client = ts.pro_api()
            client._DataApi__http_url = TS_API_URL
            return client
        except Exception as e:
            print(f"Error re-initializing Tushare client: {e}")
            return None
    return pro

def get_ts_module():
    return ts
