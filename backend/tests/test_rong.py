import requests
import pandas as pd

def get_margin_history(symbol_code):
    """
    获取单只股票的融资融券历史数据（来源：东方财富）
    symbol_code: 股票代码，如 '002371'
    """
    # 东方财富 API 接口
    url = "https://datacenter-web.eastmoney.com/api/data/v1/get"
    
    # 根据代码判断市场前缀（简单判断，可根据需要完善）
    market_type = "SZ" if symbol_code.startswith(("0", "3")) else "SH"
    
    params = {
        "reportName": "RPTA_WEB_RZRQ_GGMX",
        "columns": "ALL",
        "source": "WEB",
        "sortColumns": "DATE",
        "sortTypes": "-1",  # -1 为倒序（最新日期在前）
        "pageSize": 500,     # 获取最近 500 个交易日
        "pageNumber": 1,
        "filter": f'(SCODE="{symbol_code}")',  # 过滤单只股票
        "p": 1,
        "pageNo": 1,
        "pageNum": 1,
    }

    try:
        res = requests.get(url, params=params)
        data_json = res.json()
        
        if data_json['result'] is None:
            return pd.DataFrame()
        
        data_list = data_json['result']['data']
        df = pd.DataFrame(data_list)
        
        # 挑选并重命名核心字段
        # DATE: 日期, RZYE: 融资余额, RQYE: 融券余额, RZYZ: 融资余额占比
        # RZMRE: 融资买入额, RQYL: 融券余量
        cols_map = {
            'DATE': '交易日期',
            'SPJ': '收盘价',
            'RZYE': '融资余额(元)',
            'RQYE': '融券余额(元)',
            'RZMRE': '融资买入额(元)',
            'RCHE': '融资偿还额(元)',
            'RQYL': '融券余量(股)',
            'RQMCL': '融券卖出量(股)',
            'RQCHL': '融券偿还量(股)',
        }
        
        # 筛选存在的列
        valid_cols = [c for c in cols_map.keys() if c in df.columns]
        df = df[valid_cols].rename(columns=cols_map)
        df['交易日期'] = pd.to_datetime(df['交易日期']).dt.date
        return df

    except Exception as e:
        print(f"获取失败: {e}")
        return pd.DataFrame()

# 使用示例
df = get_margin_history("002371")
print(df.head())