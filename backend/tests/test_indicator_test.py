import unittest
import json
from unittest.mock import patch

import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import models
from database import Base
from routers.indicators import test_indicator
import schemas


class TestIndicatorTestEndpoint(unittest.TestCase):
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
        self.indicator = models.IndicatorDefinition(
            name="TestIndicator",
            akshare_api="stock_zh_a_hist",
            params_json=json.dumps({"symbol": "{symbol}"}),
            post_process_json=None,
            python_code=None,
        )
        self.db.add(self.indicator)
        self.db.commit()
        self.db.refresh(self.indicator)

    def tearDown(self):
        self.db.close()

    @patch("routers.indicators.data_fetcher.fetch")
    def test_indicator_test_ok(self, mock_fetch):
        mock_fetch.return_value = json.dumps([{"日期": "2025-01-01", "换手率": 1.23}], ensure_ascii=False)
        payload = schemas.IndicatorTestRequest(symbol="600000")
        out = test_indicator(self.indicator.id, payload, db=self.db)
        self.assertTrue(out["ok"])
        self.assertEqual(out["indicator_id"], self.indicator.id)
        self.assertEqual(out["symbol"], "600000")
        self.assertIsInstance(out["parsed"], list)

    @patch("routers.indicators.data_fetcher.fetch")
    def test_indicator_test_error(self, mock_fetch):
        mock_fetch.return_value = "Error fetching stock_zh_a_hist: boom"
        payload = schemas.IndicatorTestRequest(symbol="600000")
        out = test_indicator(self.indicator.id, payload, db=self.db)
        self.assertFalse(out["ok"])
        self.assertIn("Error", out["raw"])
        self.assertEqual(out["error"], out["raw"])


if __name__ == "__main__":
    unittest.main()
