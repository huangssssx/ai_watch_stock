import unittest
import json
from unittest.mock import patch

from database import SessionLocal, Base, engine
from models import Stock, RuleScript, Log, AIConfig, SystemConfig
from services.monitor_service import process_stock


class TestScriptOnlyRuleSignal(unittest.TestCase):
    def setUp(self):
        Base.metadata.drop_all(bind=engine)
        Base.metadata.create_all(bind=engine)
        self.db = SessionLocal()

    def tearDown(self):
        self.db.close()

    def test_script_only_uses_explicit_signal(self):
        rule = RuleScript(
            name="rule_explicit_signal",
            description="",
            code="\n".join(
                [
                    "triggered = True",
                    "signal = 'STRONG_SELL'",
                    "message = '【秃鹫卖出】清仓'",
                ]
            ),
        )
        self.db.add(rule)
        self.db.commit()

        stock = Stock(
            symbol="920662",
            name="方盛股份",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="script_only",
            rule_script_id=rule.id,
        )
        self.db.add(stock)
        self.db.commit()

        result = process_stock(
            stock.id,
            bypass_checks=True,
            send_alerts=False,
            return_result=True,
            db=self.db,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["ai_reply"]["signal"], "STRONG_SELL")

    def test_script_only_infers_signal_from_message_when_missing(self):
        rule = RuleScript(
            name="rule_infer_signal",
            description="",
            code="\n".join(
                [
                    "triggered = True",
                    "message = '【秃鹫卖出】空头控盘 | 跌破均线(32.61)'",
                ]
            ),
        )
        self.db.add(rule)
        self.db.commit()

        stock = Stock(
            symbol="920662",
            name="方盛股份",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="script_only",
            rule_script_id=rule.id,
        )
        self.db.add(stock)
        self.db.commit()

        result = process_stock(
            stock.id,
            bypass_checks=True,
            send_alerts=False,
            return_result=True,
            db=self.db,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["ai_reply"]["signal"], "SELL")

    def test_hybrid_skips_ai_when_rule_not_triggered_and_rule_signal_wait(self):
        rule = RuleScript(
            name="rule_hybrid_wait",
            description="",
            code="\n".join(
                [
                    "triggered = False",
                    "signal = 'WAIT'",
                    "message = '🔴实时 未触发：多空平衡'",
                ]
            ),
        )
        self.db.add(rule)
        self.db.commit()

        stock = Stock(
            symbol="920662",
            name="方盛股份",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="hybrid",
            rule_script_id=rule.id,
        )
        self.db.add(stock)
        self.db.commit()

        self.db.add(
            Log(
                stock_id=stock.id,
                raw_data="seed",
                ai_response="",
                ai_analysis={"type": "info", "signal": "BUY", "message": "seed"},
                is_alert=False,
            )
        )
        self.db.commit()

        with patch("services.monitor_service.ai_service.analyze_debug") as mock_analyze:
            result = process_stock(
                stock.id,
                bypass_checks=True,
                send_alerts=False,
                return_result=True,
                db=self.db,
            )
            self.assertTrue(result["ok"])
            self.assertEqual(result["skipped_reason"], "rule_not_triggered")
            mock_analyze.assert_not_called()

    def test_global_account_info_is_injected_into_ai_prompt(self):
        ai_config = AIConfig(
            name="test_ai",
            provider="openai",
            base_url="https://api.openai.com/v1",
            api_key="sk-test",
            model_name="gpt-test",
            temperature=0.1,
            max_tokens=100000,
            is_active=True,
        )
        self.db.add(ai_config)
        self.db.commit()

        self.db.add(
            SystemConfig(
                key="global_prompt",
                value=json.dumps(
                    {
                        "prompt_template": "全局策略：{{symbol}} {{name}}",
                        "account_info": "总资金: 100万; 当前持仓: {{symbol}}",
                    },
                    ensure_ascii=False,
                ),
            )
        )
        self.db.commit()

        stock = Stock(
            symbol="920662",
            name="方盛股份",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="ai_only",
            ai_provider_id=ai_config.id,
        )
        self.db.add(stock)
        self.db.commit()

        with patch("services.monitor_service.ai_service.analyze_debug") as mock_analyze:
            mock_analyze.return_value = (
                {"type": "info", "signal": "WAIT", "message": "ok"},
                "{\"ok\":true}",
                {"system_prompt": "", "user_prompt": ""},
            )
            result = process_stock(
                stock.id,
                bypass_checks=True,
                send_alerts=False,
                return_result=True,
                db=self.db,
            )
            self.assertTrue(result["ok"])
            self.assertTrue(mock_analyze.called)
            prompt_arg = mock_analyze.call_args[0][1]
            self.assertIn("【账户信息】", prompt_arg)
            self.assertIn("总资金: 100万", prompt_arg)
            self.assertIn("当前持仓: 920662", prompt_arg)

    def test_monitoring_stocks_are_injected_into_ai_prompt(self):
        ai_config = AIConfig(
            name="test_ai",
            provider="openai",
            base_url="https://api.openai.com/v1",
            api_key="sk-test",
            model_name="gpt-test",
            temperature=0.1,
            max_tokens=100000,
            is_active=True,
        )
        self.db.add(ai_config)
        self.db.commit()

        stock1 = Stock(
            symbol="920662",
            name="方盛股份",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="ai_only",
            ai_provider_id=ai_config.id,
        )
        stock2 = Stock(
            symbol="000001",
            name="平安银行",
            is_monitoring=True,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="script_only",
        )
        stock3 = Stock(
            symbol="300750",
            name="宁德时代",
            is_monitoring=False,
            interval_seconds=10,
            monitoring_schedule=json.dumps([{"start": "00:00", "end": "23:59"}]),
            monitoring_mode="ai_only",
        )
        self.db.add_all([stock1, stock2, stock3])
        self.db.commit()

        with patch("services.monitor_service.ai_service.analyze_debug") as mock_analyze:
            mock_analyze.return_value = (
                {"type": "info", "signal": "WAIT", "message": "ok"},
                "{\"ok\":true}",
                {"system_prompt": "", "user_prompt": ""},
            )
            result = process_stock(
                stock1.id,
                bypass_checks=True,
                send_alerts=False,
                return_result=True,
                db=self.db,
            )
            self.assertTrue(result["ok"])
            prompt_arg = mock_analyze.call_args[0][1]
            self.assertIn("【当前监控股票】", prompt_arg)
            self.assertIn("920662", prompt_arg)
            self.assertIn("方盛股份", prompt_arg)
            self.assertIn("000001", prompt_arg)
            self.assertIn("平安银行", prompt_arg)
            self.assertNotIn("300750", prompt_arg)
