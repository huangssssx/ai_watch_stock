import unittest
import pandas as pd
from unittest.mock import MagicMock
from overnight_arbitrage import Strategy, MarketData

class TestOvernightStrategy(unittest.TestCase):
    def setUp(self):
        self.strategy = Strategy()
        self.strategy.data_api = MagicMock()
        
    def test_dimension_a_lockin(self):
        """测试维度A：机构锁仓 (分市值)"""
        print("\n=== 测试维度 A: 机构锁仓 ===")
        
        # 构造模拟数据
        # 1. 大盘股 (600亿), 主力3.6%, 小单-2.1% -> 应该选中
        # 2. 中盘股 (100亿), 主力5.1%, 小单-3.1% -> 应该选中
        # 3. 小盘股 (40亿), 主力6.6%, 小单-4.1% -> 应该选中
        # 4. 小盘股干扰项 (40亿), 主力6.6%, 小单-1.0% -> 失败 (小单不够)
        # 5. 大盘股干扰项 (600亿), 主力2.0%, 小单-3.0% -> 失败 (主力不够)
        
        data = {
            '代码': ['000001', '000002', '000003', '000004', '000005'],
            '名称': ['大盘牛', '中盘牛', '小盘牛', '游资假动作', '大盘弱鸡'],
            '流通市值': [600e8, 100e8, 40e8, 40e8, 600e8],
            '今日主力净流入-净占比': [3.6, 5.1, 6.6, 6.6, 2.0],
            '今日小单净流入-净占比': [-2.1, -3.1, -4.1, -1.0, -3.0],
            '涨跌幅': [1.0, 1.0, 1.0, 1.0, 1.0], # 辅助字段
            '今日主力净流入-净额': [1e8, 1e8, 1e8, 1e8, 1e8] # 辅助字段
        }
        df = pd.DataFrame(data)
        
        # 运行筛选
        result = self.strategy.run_dimension_a(df)
        
        # 验证
        selected_codes = result['代码'].tolist() if not result.empty else []
        print(f"选中代码: {selected_codes}")
        
        self.assertIn('000001', selected_codes, "大盘股应被选中")
        self.assertIn('000002', selected_codes, "中盘股应被选中")
        self.assertIn('000003', selected_codes, "小盘股应被选中")
        self.assertNotIn('000004', selected_codes, "游资假动作(小单不足)不应被选中")
        self.assertNotIn('000005', selected_codes, "弱势大盘股不应被选中")

    def test_dimension_b_reversal(self):
        """测试维度B：低位弹性起爆"""
        print("\n=== 测试维度 B: 低位起爆 ===")
        
        # 构造数据
        # 1. 完美起爆: 涨1%, 流入600万, 昨日流出-600万 -> 选中
        # 2. 涨幅过大: 涨4%, 流入600万 -> 失败
        # 3. 流入不足: 涨1%, 流入400万 -> 失败
        # 4. 假反转: 涨1%, 流入600万, 昨日流入+100万 -> 失败
        
        data = {
            '代码': ['100001', '100002', '100003', '100004'],
            '名称': ['完美起爆', '涨幅过大', '流入不足', '假反转'],
            '涨跌幅': [1.0, 4.0, 1.0, 1.0],
            '今日主力净流入-净额': [6000000, 6000000, 4000000, 6000000],
            # 维度A字段填充以防报错
            '流通市值': [100e8]*4, '今日主力净流入-净占比': [0]*4, '今日小单净流入-净占比': [0]*4
        }
        df = pd.DataFrame(data)
        
        # Mock 历史数据
        def mock_history(symbol):
            if symbol == '100001': return -6000000
            if symbol == '100004': return 1000000
            return 0
            
        self.strategy.data_api.get_history_flow_yesterday = MagicMock(side_effect=mock_history)
        
        # 运行
        result = self.strategy.run_dimension_b(df)
        
        selected = result['代码'].tolist() if not result.empty else []
        print(f"选中代码: {selected}")
        
        self.assertIn('100001', selected)
        self.assertNotIn('100002', selected)
        self.assertNotIn('100003', selected)
        self.assertNotIn('100004', selected)

    def test_dimension_c_sector_leader(self):
        """测试维度C：分歧龙头"""
        print("\n=== 测试维度 C: 分歧龙头 ===")
        
        # 1. 构造板块数据
        # 银行: -1% (符合)
        # 科技: -4% (跌太多)
        # 医药: +1% (涨了)
        sectors = pd.DataFrame({
            '板块名称': ['银行', '科技', '医药'],
            '涨跌幅': [-1.0, -4.0, 1.0]
        })
        self.strategy.data_api.get_sector_list.return_value = sectors
        
        # 2. 构造板块成分股
        # 银行成分股: 
        #   - 招行: 流入2000万 (排名1, 符合)
        #   - 兴业: 流入1500万 (排名2, 符合)
        #   - 平安: 流入1200万 (排名3, 符合)
        #   - 浦发: 流入2000万 (排名4, 虽然钱多但排名低? 不, 逻辑是前3)
        #     注: 代码中是先排序再取head(3)。所以前3名只要大于1000万都行。
        #   - 宁波: 流入500万 (钱不够)
        
        # Mock get_sector_cons
        sector_cons_map = {
            '银行': ['600036', '601166', '000001', '600000', '002142'],
            '科技': ['T1'], '医药': ['M1']
        }
        self.strategy.data_api.get_sector_cons = MagicMock(side_effect=lambda x: sector_cons_map.get(x, []))
        
        # 3. 构造全市场Merged Data (包含所有成分股详情)
        data = {
            '代码': ['600036', '601166', '000001', '600000', '002142', 'T1', 'M1'],
            '名称': ['招行', '兴业', '平安', '浦发', '宁波', '科技股', '医药股'],
            '今日主力净流入-净额': [20000000, 15000000, 12000000, 11000000, 5000000, 100000000, 100000000],
            # 填充其他
            '涨跌幅': [0]*7, '流通市值': [100e8]*7, '今日主力净流入-净占比': [0]*7, '今日小单净流入-净占比': [0]*7
        }
        df_merged = pd.DataFrame(data)
        
        # 运行
        result = self.strategy.run_dimension_c(df_merged)
        
        selected = result['代码'].tolist() if not result.empty else []
        print(f"选中代码: {selected}")
        
        # 验证银行股
        self.assertIn('600036', selected, "招行应选中") # Rank 1
        self.assertIn('601166', selected, "兴业应选中") # Rank 2
        self.assertIn('000001', selected, "平安应选中") # Rank 3
        self.assertNotIn('600000', selected, "浦发(Rank 4)不应选中")
        self.assertNotIn('002142', selected, "宁波(金额不足)不应选中")
        
        # 验证其他板块
        self.assertNotIn('T1', selected, "科技板块跌幅过大不应选中")
        self.assertNotIn('M1', selected, "医药板块上涨不应选中")

if __name__ == '__main__':
    unittest.main()
