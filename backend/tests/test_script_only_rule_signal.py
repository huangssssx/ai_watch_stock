import unittest
import json

from database import SessionLocal, Base, engine
from models import Stock, RuleScript
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

