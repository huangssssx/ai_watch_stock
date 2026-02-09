"""
股票代码工具（A 股 / pytdx）

用途：
- 提供“全市场 A 股股票列表”的统一获取入口（market, code, name）
- 内置按天 CSV 缓存，避免每次运行都全量请求 pytdx
- 统一 A 股代码前缀过滤逻辑（粗过滤，排除基金/债券/指数等）

数据来源：
- pytdx：get_security_count / get_security_list

使用建议：
- 业务/脚本中尽量只调用 get_all_a_share_codes() 获取股票池
- 需要自定义缓存位置或黑名单时，可在此文件扩展参数化能力
"""

import os
import sys
from typing import Optional
import pandas as pd

# 黑名单：剔除不参与计算/回测的标的（例如新股/异常标的等）
blacklist = ["603284", "688712", "688816", "688818"]

# 让脚本可以从 backend/scripts 直接运行并 import backend/utils 下的工具
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

from utils.pytdx_client import tdx

def stock_code_cache_name(today: Optional[pd.Timestamp] = None) -> str:
    """生成“按天缓存”的 CSV 文件名。

    约定：
    - 文件名形如 all_a_share_codes_cache_YYYYMMDD.csv
    - 默认写入/读取当前工作目录（如需固定目录，可在此处改为绝对路径）

    参数：
    - today：用于测试或回放的日期；不传则使用当前日期
    """
    date = today or pd.Timestamp.today()
    return f"all_a_share_codes_cache_{date.strftime('%Y%m%d')}.csv"

def is_a_share_stock(market: int, code: str) -> bool:
    """判断 (market, code) 是否为 A 股“股票”代码（粗过滤）。

    - market=0：深市；market=1：沪市
    - 通过代码前缀做快速过滤，用于排除基金/债券/指数等非股票品种

    注意：
    - 该方法是“启发式规则”，并非交易所官方口径；若后续要更严谨，
      建议结合证券类型字段（若数据源提供）做二次过滤。
    """

    code = str(code or "").zfill(6)
    if market == 0:
        # 深市主板/中小板/创业板（含 301）等
        return code.startswith(("000", "001", "002", "003", "300", "301"))
    if market == 1:
        # 沪市主板/科创板
        return code.startswith(("600", "601", "603", "605", "688"))
    return False

def iter_all_a_share_codes():
    """遍历全市场 A 股 (market, code, name)。

    实现方式：
    - 先按 market 分市场获取证券总数：get_security_count
    - 再按分页获取列表：get_security_list(market, start)
    - 对每条记录做 A 股代码前缀过滤：is_a_share_stock

    返回：
    - 生成器：逐条 yield (market, code, name)
    """
    for market in (0, 1):  # 0=深, 1=沪
        total = tdx.get_security_count(market)
        step = 1000  # 常见每页1000
        for start in range(0, total, step):
            rows = tdx.get_security_list(market, start) or []
            # print(rows)
            for r in rows:
                code = str(r.get("code", "")).zfill(6)
                name = str(r.get("name", "")).strip()
                if code and is_a_share_stock(market, code):
                    yield (market, code, name)

def load_stock_codes(cache_file: str) -> pd.DataFrame:
    """读取股票列表：优先读缓存；没有则全市场拉取并写入缓存。

    参数：
    - cache_file：缓存文件路径（通常为 stock_code_cache_name() 的返回值）

    返回：
    - DataFrame，列：market, code, name

    说明：
    - 当缓存不存在时会触发全市场请求（耗时较长且依赖行情服务器状态）
    - 会应用 blacklist 做剔除（仅按 code 字段）
    """
    if not os.path.exists(cache_file):
        df_stock_codes = pd.DataFrame(iter_all_a_share_codes(), columns=["market", "code", "name"])
        df_stock_codes = df_stock_codes[~df_stock_codes["code"].isin(blacklist)]
        df_stock_codes.to_csv(cache_file, index=False)
        return df_stock_codes
    return pd.read_csv(cache_file)

def normalize_stock_codes(df_stock_codes: pd.DataFrame) -> pd.DataFrame:
    """规范化字段类型，保证 market 为 int、code 为 6 位字符串。

    为什么需要：
    - CSV 读写可能导致 market/code 类型漂移（如 int/str 混用）
    - 下游接口与 merge/过滤逻辑一般依赖 code 为 6 位字符串
    """
    df_stock_codes["market"] = df_stock_codes["market"].astype(int)
    df_stock_codes["code"] = df_stock_codes["code"].astype(str).str.zfill(6)
    return df_stock_codes

def get_all_a_share_codes() -> pd.DataFrame:
    """获取全市场 A 股股票列表（包含 market, code, name 三列）。

    行为：
    - 先根据当天日期决定缓存文件名
    - 若缓存存在：直接读取
    - 若缓存不存在：全市场拉取 -> 应用 blacklist -> 写入缓存
    - 最后做一次字段规范化
    """
    cache_file = stock_code_cache_name()
    return normalize_stock_codes(load_stock_codes(cache_file))


def main():
    """命令行入口：用于手动验证/刷新 A 股代码缓存。"""
    cache_file = stock_code_cache_name()
    print(f"缓存文件：{cache_file}")
    try:
        df_stock_codes = get_all_a_share_codes()
        print(df_stock_codes)
    except Exception as e:
        print(f"获取股票列表失败：{e}")

if __name__ == "__main__":
    main()
