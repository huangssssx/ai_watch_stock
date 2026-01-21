import unittest
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch, MagicMock
import pandas as pd
from services.data_fetcher import DataFetcher

class TestDataFetcherPython(unittest.TestCase):
    def setUp(self):
        self.fetcher = DataFetcher()

    def test_fetch_with_python_code(self):
        mock_pro = MagicMock()
        mock_df = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'open': [100, 102],
            'close': [102, 105],
            'high': [105, 106],
            'low': [99, 101]
        })
        mock_pro.dummy_api.return_value = mock_df
        self.fetcher.pro = mock_pro

        # Python code to calculate a simple indicator (e.g., mean of close)
        python_code = """
df['mean_close'] = df['close'].mean()
df = df[['date', 'mean_close']]
"""
        
        # Call fetch
        result_json = self.fetcher.fetch(
            api_name='dummy_api',
            params_json='{}',
            context={},
            python_code=python_code
        )
        
        # Verify result
        result_df = pd.read_json(result_json)
        self.assertTrue('mean_close' in result_df.columns)
        self.assertTrue('date' in result_df.columns)
        self.assertFalse('open' in result_df.columns) # Should be filtered out
        self.assertEqual(result_df['mean_close'].iloc[0], 103.5)

    def test_post_process_has_context_dict(self):
        mock_pro = MagicMock()
        mock_df = pd.DataFrame(
            {
                "date": ["2023-01-01", "2023-01-02"],
                "close": [102, 105],
            }
        )
        mock_pro.dummy_api.return_value = mock_df
        self.fetcher.pro = mock_pro

        python_code = """
symbol = context["symbol"]
df["symbol"] = symbol
"""

        result_json = self.fetcher.fetch(
            api_name="dummy_api",
            params_json="{}",
            context={"symbol": "600000"},
            python_code=python_code,
        )

        result_df = pd.read_json(result_json)
        self.assertIn("symbol", result_df.columns)
        self.assertEqual(str(result_df["symbol"].iloc[0]), "600000")

    def test_pure_script_injects_context_keys(self):
        python_code = """
result = {"symbol": symbol, "name": name}
"""
        result_json = self.fetcher.fetch(
            api_name=None,
            params_json=None,
            context={"symbol": "600000", "name": "浦发银行"},
            python_code=python_code,
        )
        result = __import__("json").loads(result_json)
        self.assertEqual(result["symbol"], "600000")
        self.assertEqual(result["name"], "浦发银行")

    def test_fetch_atr_logic(self):
        mock_pro = MagicMock()
        mock_df = pd.DataFrame({
            '日期': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            '最高': [10.0, 12.0, 13.0, 14.0],
            '最低': [8.0, 10.0, 11.0, 12.0],
            '收盘': [9.0, 11.0, 12.0, 13.0]
        })
        mock_pro.dummy_api.return_value = mock_df
        self.fetcher.pro = mock_pro

        # Python code for ATR (Wilder's Smoothing)
        python_code = """
# 确保按日期排序
df = df.sort_values(by="日期")

# 计算 TR
high = df["最高"]
low = df["最低"]
prev_close = df["收盘"].shift(1)

tr1 = high - low
tr2 = (high - prev_close).abs()
tr3 = (low - prev_close).abs()

# 取三者最大值
df["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

# 计算 ATR (N=14)，使用 Wilder's Smoothing (alpha=1/N)
# 这里数据量少，为了测试有结果，我们用 N=2
df["ATR"] = df["TR"].ewm(alpha=1/2, adjust=False).mean()

# 格式化日期
df["日期"] = pd.to_datetime(df["日期"]).dt.strftime('%Y-%m-%d')
"""

        result_json = self.fetcher.fetch(
            api_name='dummy_api',
            params_json='{}',
            context={},
            python_code=python_code
        )
        
        result_df = pd.read_json(result_json)
        self.assertIn('ATR', result_df.columns)
        self.assertIn('TR', result_df.columns)
        # Check execution didn't fail
        self.assertEqual(len(result_df), 4)

if __name__ == '__main__':
    unittest.main()
