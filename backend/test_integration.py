import unittest
from unittest.mock import MagicMock, patch
import os
from services.data_fetcher import DataFetcher
from services.ai_service import AIService
from services.monitor_service import process_stock
from models import Stock, IndicatorDefinition, AIConfig
from database import SessionLocal, Base, engine

# Mock Akshare
class TestIntegration(unittest.TestCase):
    def setUp(self):
        db_path = os.path.join(os.path.dirname(__file__), "stock_watch.db")
        if os.path.exists(db_path):
            os.remove(db_path)
        Base.metadata.create_all(bind=engine)
        self.db = SessionLocal()
        
        # Setup Test Data
        self.ai_config = AIConfig(name="TestAI", provider="openai", base_url="http://test", api_key="sk-test", model_name="gpt-3.5")
        self.db.add(self.ai_config)
        self.db.commit()
        
        self.stock = Stock(symbol="600000", name="PF Bank", is_monitoring=True, interval_seconds=10, ai_provider_id=self.ai_config.id)
        self.db.add(self.stock)
        self.db.commit()
        
        self.indicator = IndicatorDefinition(name="Spot", akshare_api="stock_zh_a_spot_em", params_json='{"symbol": "{symbol}"}')
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
        mock_ak.stock_zh_a_spot_em.return_value.to_csv.return_value = "symbol,price\n600000,10.5"
        
        # 2. Mock AI Response
        mock_client = MagicMock()
        mock_openai.return_value = mock_client
        mock_response = MagicMock()
        mock_response.choices[0].message.content = '{"type": "warning", "message": "Price drop"}'
        mock_client.chat.completions.create.return_value = mock_response
        
        # 3. Run Process
        process_stock(self.stock.id)
        
        # 4. Verify Log
        from models import Log
        log = self.db.query(Log).filter(Log.stock_id == self.stock.id).first()
        self.assertIsNotNone(log)
        self.assertTrue(log.is_alert)
        self.assertEqual(log.ai_analysis['type'], 'warning')
        print("Integration test passed: Flow executed successfully and alert logged.")

if __name__ == '__main__':
    unittest.main()
