# ---------------------------------------------------------
# 指标名称：个股筹码分布分析
# 运行模式：纯 Python 脚本模式
# 系统注入：ak (Akshare), pd (Pandas), context (上下文信息)
# ---------------------------------------------------------

# 1. 从上下文 context 获取当前股票代码
# 示例：context['symbol'] = "002371"
import os
import sys

backend_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from pymr_compat import ensure_py_mini_racer

ensure_py_mini_racer()
import akshare as ak
import pandas as pd
stock_code = "000725"

if stock_code:
    try:
        # 2. 调用 Akshare 获取个股筹码分布数据
        # 注意：在 Akshare 1.17.96+ 版本中，接口名为 stock_cyq_em
        # 该接口返回包含：日期、获利比例、平均成本、90%筹码集中度等字段
        chip_df = ak.stock_cyq_em(symbol=stock_code)
        
        # 3. 数据清洗与处理
        if not chip_df.empty:
            # 默认返回完整历史数据
            # 如果您只需要最新的一天，可以使用：df = chip_df.tail(1)
            df = chip_df
        else:
            # 若无数据，返回一个包含提示信息的 DataFrame
            df = pd.DataFrame([{"状态": "无数据", "代码": stock_code}])
            
    except Exception as e:
        # 异常处理：返回错误信息，方便在系统中查看具体原因
        df = pd.DataFrame([{"错误": f"接口调用失败: {str(e)}", "代码": stock_code}])
else:
    # 如果没有获取到代码，返回提示
    df = pd.DataFrame([{"提示": "未检测到股票代码"}])
print(df)
