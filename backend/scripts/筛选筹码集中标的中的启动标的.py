import pandas as pd
import os
import sys
import time
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
# 导入tushare客户端
from backend.utils.tushare_client import pro

# 检查pro是否初始化成功
if pro is None:
    print("错误：Tushare客户端初始化失败，无法获取资金流向数据！")
    sys.exit(1)

# 定义文件路径
input_file = '/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/筹码忽然集中_平衡型_20260211.csv'
output_file = '/Users/huangchuanjian/workspace/my_projects/ai_watch_stock/筹码集中启动标的_20260211.csv'

# 计算最近的交易日
def get_latest_trading_date():
    try:
        # 从CSV文件中获取日期信息
        # 读取CSV文件的日期列
        date_df = pd.read_csv(input_file, usecols=['start_date', 'end_date'])
        # 获取最大的日期
        max_start_date = date_df['start_date'].max()
        max_end_date = date_df['end_date'].max()
        # 使用最大的日期作为参考
        latest_date = max(max_start_date, max_end_date)
        print(f"从CSV文件获取的最新日期: {latest_date}")
        return latest_date
    except Exception as e:
        print(f"从CSV文件获取日期失败: {e}")
        # 退回到默认方式
        try:
            # 获取当前日期
            today = datetime.now().strftime('%Y%m%d')
            # 获取交易日历
            df = pro.trade_cal(exchange='SSE', start_date=(datetime.now() - timedelta(days=30)).strftime('%Y%m%d'), end_date=today)
            trading_dates = df[df['is_open'] == 1]['cal_date'].tolist()
            return trading_dates[-1] if trading_dates else today
        except Exception as e:
            print(f"获取交易日失败: {e}")
            return datetime.now().strftime('%Y%m%d')

# 获取股票资金流向数据
def get_money_flow_data(ts_codes, trade_date):
    """获取股票资金流向数据"""
    try:
        # 确保trade_date是字符串类型
        trade_date_str = str(trade_date)
        # 分批获取数据，每次最多200只股票
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.moneyflow_dc(ts_code=','.join(batch_codes), trade_date=trade_date_str)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取资金流向数据失败: {e}")
        return pd.DataFrame()

# 获取股票成交量数据
def get_volume_data(ts_codes, trade_date, days=5):
    """获取股票成交量数据，计算成交量放大情况"""
    try:
        # 确保trade_date是字符串类型
        trade_date_str = str(trade_date)
        # 计算开始日期
        end_date = trade_date_str
        start_date = (datetime.strptime(trade_date_str, '%Y%m%d') - timedelta(days=days)).strftime('%Y%m%d')
        
        # 分批获取数据
        batch_size = 200
        result = []
        
        for i in range(0, len(ts_codes), batch_size):
            batch_codes = ts_codes[i:i+batch_size]
            df = pro.daily(ts_code=','.join(batch_codes), start_date=start_date, end_date=end_date)
            result.append(df)
            time.sleep(0.5)  # 避免请求过于频繁
        
        if result:
            return pd.concat(result, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        print(f"获取成交量数据失败: {e}")
        return pd.DataFrame()

# 获取北向资金持股数据（已移除）
def get_hsgt_hold_data(ts_codes, trade_date):
    """获取北向资金持股数据（已移除）"""
    return pd.DataFrame()

# 检查输入文件是否存在
if not os.path.exists(input_file):
    print(f"错误：输入文件 {input_file} 不存在！")
    exit(1)

# 读取CSV文件
print(f"正在读取文件：{input_file}")
df = pd.read_csv(input_file)

# 打印原始数据行数
print(f"原始数据行数：{len(df)}")

# 筛选条件
print("\n筛选条件：")
print("1. 成本范围变化率 < -10% （筹码集中程度更高）")
print("2. 胜率变化率 > 30% （市场情绪向好更明显）")
print("3. 股价与加权平均成本差异 < 5% （主力未大幅获利，拉抬动力足）")

# 应用筛选条件
filtered_df = df[
    (df['cost_range_change_rate'] < -10) &  # 成本范围变化率 < -10%
    (df['winner_rate_change_rate'] > 30) &  # 胜率变化率 > 30%
    (df['weight_avg_diff_percent'] < 5)      # 股价与加权平均成本差异 < 5%
]

# 打印筛选后的数据行数
print(f"\n筛选后数据行数：{len(filtered_df)}")

# 按成本范围变化率降序排序（负值越小，集中程度越高）
filtered_df = filtered_df.sort_values(by='cost_range_change_rate', ascending=True)

# 重置索引
filtered_df = filtered_df.reset_index(drop=True)

# 获取最近的交易日
latest_trading_date = get_latest_trading_date()
print(f"\n使用最近交易日数据：{latest_trading_date}")

# 如果有筛选结果，进行资金流向和成交量核查
if len(filtered_df) > 0:
    # 提取股票代码列表
    stock_codes = filtered_df['ts_code'].tolist()
    print(f"\n正在获取 {len(stock_codes)} 只股票的资金流向和成交量数据...")
    
    # 获取资金流向数据
    money_flow_df = get_money_flow_data(stock_codes, latest_trading_date)
    
    # 获取成交量数据
    volume_df = get_volume_data(stock_codes, latest_trading_date)
    
    # 获取北向资金数据
    hsgt_hold_df = get_hsgt_hold_data(stock_codes, latest_trading_date)
    
    # 合并数据
    if not money_flow_df.empty:
        filtered_df = filtered_df.merge(money_flow_df[['ts_code', 'net_amount', 'net_amount_rate']], on='ts_code', how='left')
    else:
        filtered_df['net_amount'] = 0
        filtered_df['net_amount_rate'] = 0
    
    # 计算成交量放大情况
    if not volume_df.empty:
        # 计算每只股票的平均成交量和当日成交量
        volume_stats = volume_df.groupby('ts_code').agg(
            avg_volume=('vol', 'mean'),
            latest_volume=('vol', 'last')
        ).reset_index()
        # 计算成交量放大倍数
        volume_stats['volume_ratio'] = volume_stats['latest_volume'] / volume_stats['avg_volume']
        # 合并到主数据框
        filtered_df = filtered_df.merge(volume_stats[['ts_code', 'volume_ratio', 'latest_volume']], on='ts_code', how='left')
    else:
        filtered_df['volume_ratio'] = 1.0
        filtered_df['latest_volume'] = 0
    
    # 合并北向资金数据
    if not hsgt_hold_df.empty:
        filtered_df = filtered_df.merge(hsgt_hold_df[['ts_code', 'hold_amount', 'hold_ratio']], on='ts_code', how='left')
    else:
        filtered_df['hold_amount'] = 0
        filtered_df['hold_ratio'] = 0
    
    # 应用资金流向和成交量筛选条件
    print("\n应用资金流向和成交量筛选条件：")
    print("1. 主力净流入 > 0 （资金流入）")
    print("2. 成交量放大倍数 > 1.2 （有量）")
    
    final_df = filtered_df[
        (filtered_df['net_amount'] > 0) &  # 主力净流入
        (filtered_df['volume_ratio'] > 1.2)  # 成交量放大
    ]
    
    # 按主力净流入和成交量放大倍数排序
    final_df = final_df.sort_values(by=['net_amount', 'volume_ratio'], ascending=[False, False])
    final_df = final_df.reset_index(drop=True)
    
    # 保存最终结果
    print(f"\n正在保存最终筛选结果到：{output_file}")
    final_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # 打印前10行结果
    print("\n最终筛选结果前10行：")
    print(final_df.head(10))
    
    # 打印总结
    print(f"\n筛选完成！共筛选出 {len(final_df)} 只符合条件的股票。")
    print(f"结果已保存到：{output_file}")
    
    # 打印资金流向和成交量统计
    if not final_df.empty:
        print("\n资金流向和成交量统计：")
        print(f"平均主力净流入：{final_df['net_amount'].mean():.2f} 万元")
        print(f"平均成交量放大倍数：{final_df['volume_ratio'].mean():.2f} 倍")
else:
    # 保存空结果
    print(f"\n正在保存筛选结果到：{output_file}")
    filtered_df.to_csv(output_file, index=False, encoding='utf-8')
    
    # 打印总结
    print(f"\n筛选完成！共筛选出 {len(filtered_df)} 只符合条件的股票。")
    print(f"结果已保存到：{output_file}")