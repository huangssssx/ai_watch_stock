import unittest
from unittest.mock import MagicMock, patch
import os
import sys
import json

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.data_fetcher import DataFetcher
from services.ai_service import AIService
from services.monitor_service import process_stock
from models import Stock, IndicatorDefinition, AIConfig
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from database import Base

# Mock Akshare
class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        Base.metadata.drop_all(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        self.db = self.SessionLocal()
        
        # Setup Test Data
        self.ai_config = AIConfig(name="TestAI", provider="openai", base_url="http://test", api_key="sk-test", model_name="gpt-3.5")
        self.db.add(self.ai_config)
        self.db.commit()
        
        self.stock = Stock(
            symbol="600000",
            name="PF Bank",
            is_monitoring=True,
            interval_seconds=10,
            ai_provider_id=self.ai_config.id,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            only_trade_days=False,
        )
        self.db.add(self.stock)
        self.db.commit()
        
        self.indicator = IndicatorDefinition(name="Spot", akshare_api="stock_zh_a_spot_em", params_json="{}")
        self.db.add(self.indicator)
        self.db.commit()
        self.stock.indicators = [self.indicator]
        self.db.commit()

    def tearDown(self):
        self.db.query(IndicatorDefinition).delete()
        self.db.query(Stock).delete()
        self.db.query(AIConfig).delete()
        self.db.commit()
        self.db.close()

    @patch('services.data_fetcher.ak')
    @patch('services.ai_service.OpenAI')
    @patch('services.alert_service.smtplib')
    def test_process_loop(self, mock_smtp, mock_openai, mock_ak):
        # 1. Mock Data Fetch
        import pandas as pd
        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame([{"symbol": "600000", "price": 10.5}])
        
        # 2. Mock AI Response
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"type": "warning", "message": "Price drop"}'
        mock_client.chat.completions.create.return_value = mock_response
        
        # 3. Run Process
        process_stock(self.stock.id, bypass_checks=True, send_alerts=True, is_test=False, return_result=True, db=self.db)
        
        # 4. Verify Log
        from models import Log
        log = self.db.query(Log).filter(Log.stock_id == self.stock.id).first()
        self.assertIsNotNone(log)
        self.assertTrue(log.is_alert)
        self.assertEqual(log.ai_analysis['type'], 'warning')
        print("Integration test passed: Flow executed successfully and alert logged.")

    @patch('services.data_fetcher.ak')
    @patch('services.ai_service.OpenAI')
    @patch('services.monitor_service.alert_service.send_email')
    def test_test_run_can_send_email_when_enabled(self, mock_send_email, mock_openai, mock_ak):
        import pandas as pd
        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame([{"symbol": "600000", "price": 10.5}])

        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(
            {
                "type": "info",
                "signal": "BUY",
                "action_advice": "Test buy",
                "suggested_position": "1成仓",
                "duration": "1天",
                "stop_loss_price": 9.9,
                "message": "Test message",
            }
        )
        mock_client.chat.completions.create.return_value = mock_response

        mock_send_email.return_value = {"ok": True, "mocked": True, "receiver_email": "test@example.com", "error": None}

        result = process_stock(self.stock.id, send_alerts=True, is_test=False, return_result=True, db=self.db)

        self.assertIsNotNone(result)
        self.assertTrue(result["ok"])
        self.assertTrue(result["is_alert"])
        self.assertTrue(result["alert_attempted"])
        mock_send_email.assert_called_once()

    @patch('services.data_fetcher.ak')
    @patch('services.ai_service.OpenAI')
    def test_bypass_checks_runs_even_if_not_monitoring(self, mock_openai, mock_ak):
        import pandas as pd
        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame([{"symbol": "600000", "price": 10.5}])

        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = json.dumps(
            {
                "type": "info",
                "signal": "WAIT",
                "action_advice": "Test",
                "suggested_position": "-",
                "duration": "1天",
                "stop_loss_price": 9.9,
                "message": "Test message",
            }
        )
        mock_client.chat.completions.create.return_value = mock_response

        self.stock.is_monitoring = False
        self.db.add(self.stock)
        self.db.commit()

        result = process_stock(self.stock.id, bypass_checks=True, send_alerts=False, is_test=False, return_result=True, db=self.db)

        self.assertIsNotNone(result)
        self.assertTrue(result["ok"])
        self.assertFalse(bool(result.get("skipped_reason")))

    @patch('services.ai_service.OpenAI')
    def test_chat_uses_system_role_for_openai_base_url(self, mock_openai):
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "ok"
        mock_client.chat.completions.create.return_value = mock_response

        svc = AIService()
        ai_config = {"api_key": "sk-test", "base_url": "https://api.openai.com/v1", "model_name": "gpt-4o-mini"}
        out = svc.chat("hello", ai_config, system_prompt="请在结尾加“主人！”")
        self.assertEqual(out, "ok")

        _, kwargs = mock_client.chat.completions.create.call_args
        self.assertEqual(kwargs["messages"][0]["role"], "system")
        self.assertIn("主人", kwargs["messages"][0]["content"])

    @patch('services.ai_service.OpenAI')
    def test_chat_embeds_system_prompt_for_non_openai_base_url(self, mock_openai):
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "ok"
        mock_client.chat.completions.create.return_value = mock_response

        svc = AIService()
        ai_config = {"api_key": "sk-test", "base_url": "https://api.deepseek.com/v1", "model_name": "deepseek-chat"}
        out = svc.chat("hello", ai_config, system_prompt="请在结尾加“主人！”")
        self.assertEqual(out, "ok")

        _, kwargs = mock_client.chat.completions.create.call_args
        self.assertEqual(kwargs["messages"][0]["role"], "user")
        self.assertIn("系统指令", kwargs["messages"][0]["content"])
        self.assertIn("主人", kwargs["messages"][0]["content"])

class TestPostProcess(unittest.TestCase):
    def test_filter_rows_numeric_compare(self):
        import pandas as pd

        fetcher = DataFetcher()
        df = pd.DataFrame(
            {
                "日期": ["2025-01-01", "2025-01-02", "2025-01-03"],
                "份额变化": ["1,000", "200", "--"],
            }
        )
        spec = {"filter_rows": [{"column": "份额变化", "op": ">", "value": 500}]}
        out = fetcher._apply_post_process(df, json.dumps(spec))
        self.assertEqual(out["日期"].tolist(), ["2025-01-01"])

    def test_filter_rows_contains_and_in(self):
        import pandas as pd

        fetcher = DataFetcher()
        df = pd.DataFrame(
            {
                "基金简称": ["A股ETF", "中证500ETF", "可转债基金"],
                "代码": ["510300", "510500", "000000"],
            }
        )
        spec = {
            "filter_rows": {
                "and": [
                    {"column": "基金简称", "op": "contains", "value": "ETF"},
                    {"column": "代码", "op": "in", "value": ["510300"]},
                ]
            }
        }
        out = fetcher._apply_post_process(df, json.dumps(spec))
        self.assertEqual(out["代码"].tolist(), ["510300"])

    @patch('services.data_fetcher.ak')
    def test_fetch_resolves_placeholders_in_post_process(self, mock_ak):
        import pandas as pd

        mock_ak.stock_zh_a_spot_em.return_value = pd.DataFrame(
            {
                "代码": ["600000", "000001"],
                "基金简称": ["X", "Y"],
                "日期": ["2025-01-01", "2025-01-01"],
                "份额变化": ["100", "200"],
            }
        )

        fetcher = DataFetcher()
        out_json = fetcher.fetch(
            "stock_zh_a_spot_em",
            params_json='{"symbol":"{symbol}"}',
            context={"symbol": "600000"},
            post_process_json=json.dumps(
                {
                    "filter_rows": [{"column": "代码", "op": "in", "value": ["{symbol}"]}],
                    "select_columns": ["代码", "基金简称", "日期", "份额变化"],
                },
                ensure_ascii=False,
            ),
        )
        self.assertIn("600000", out_json)
        self.assertNotIn("000001", out_json)

if __name__ == '__main__':
    unittest.main()
