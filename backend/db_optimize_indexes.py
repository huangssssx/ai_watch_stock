"""
数据库索引优化脚本
为高频查询添加复合索引，提升查询性能

使用方法:
    python db_optimize_indexes.py
"""

from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker
from database import SQLALCHEMY_DATABASE_URL, Base
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseIndexOptimizer:
    """数据库索引优化器"""

    def __init__(self, db_url: str = SQLALCHEMY_DATABASE_URL):
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def get_existing_indexes(self, table_name: str) -> list[str]:
        """获取表上已有的索引"""
        inspector = inspect(self.engine)
        indexes = inspector.get_indexes(table_name)
        return [idx['name'] for idx in indexes]

    def index_exists(self, table_name: str, index_name: str) -> bool:
        """检查索引是否存在"""
        existing = self.get_existing_indexes(table_name)
        return index_name in existing

    def create_index_if_not_exists(
        self,
        table_name: str,
        index_name: str,
        columns: list[str],
        unique: bool = False
    ) -> bool:
        """
        创建索引（如果不存在）

        Returns:
            bool: 是否创建了新索引
        """
        if self.index_exists(table_name, index_name):
            logger.info(f"Index {index_name} already exists on {table_name}")
            return False

        column_list = ', '.join(columns)
        unique_str = "UNIQUE " if unique else ""

        sql = f"""
            CREATE {unique_str}INDEX IF NOT EXISTS {index_name}
            ON {table_name} ({column_list})
        """

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"Created index {index_name} on {table_name} ({column_list})")
            return True
        except Exception as e:
            logger.error(f"Failed to create index {index_name}: {e}")
            return False

    def optimize_all_tables(self) -> dict[str, int]:
        """
        优化所有表的索引

        Returns:
            dict: 每个表创建的索引数量
        """
        results = {}

        # 1. logs 表优化
        results['logs'] = self._optimize_logs_table()

        # 2. stock_news 表优化
        results['stock_news'] = self._optimize_stock_news_table()

        # 3. sentiment_analysis 表优化
        results['sentiment_analysis'] = self._optimize_sentiment_analysis_table()

        # 4. screener_results 表优化
        results['screener_results'] = self._optimize_screener_results_table()

        # 5. stocks 表优化
        results['stocks'] = self._optimize_stocks_table()

        return results

    def _optimize_logs_table(self) -> int:
        """优化 logs 表索引"""
        count = 0

        # logs 表常用查询：
        # 1. 按 stock_id 查询最近的日志
        # 2. 按 stock_id 和 timestamp 排序查询
        # 3. 按 is_alert 过滤
        # 4. 按时间范围查询

        indexes = [
            # stock_id 和 timestamp 的复合索引（覆盖最常见查询）
            ('idx_logs_stock_timestamp', ['stock_id', 'timestamp']),
            # is_alert 索引（用于过滤告警）
            ('idx_logs_is_alert', ['is_alert']),
            # timestamp 降序索引（用于时间范围查询）
            ('idx_logs_timestamp_desc', ['timestamp DESC']),
            # stock_id 和 is_alert 复合索引
            ('idx_logs_stock_alert', ['stock_id', 'is_alert']),
        ]

        for name, columns in indexes:
            if self.create_index_if_not_exists('logs', name, columns):
                count += 1

        return count

    def _optimize_stock_news_table(self) -> int:
        """优化 stock_news 表索引"""
        count = 0

        indexes = [
            # publish_time 索引（按时间查询新闻）
            ('idx_news_publish_time', ['publish_time']),
            # source 索引（按来源筛选）
            ('idx_news_source', ['source']),
        ]

        for name, columns in indexes:
            if self.create_index_if_not_exists('stock_news', name, columns):
                count += 1

        return count

    def _optimize_sentiment_analysis_table(self) -> int:
        """优化 sentiment_analysis 表索引"""
        count = 0

        indexes = [
            # target_type 和 target_value 复合索引（按目标筛选）
            ('idx_sentiment_target', ['target_type', 'target_value']),
            # timestamp 索引（按时间查询）
            ('idx_sentiment_timestamp', ['timestamp']),
        ]

        for name, columns in indexes:
            if self.create_index_if_not_exists('sentiment_analysis', name, columns):
                count += 1

        return count

    def _optimize_screener_results_table(self) -> int:
        """优化 screener_results 表索引"""
        count = 0

        indexes = [
            # screener_id 和 run_at 复合索引（查询选股结果）
            ('idx_screener_results_run', ['screener_id', 'run_at DESC']),
        ]

        for name, columns in indexes:
            if self.create_index_if_not_exists('screener_results', name, columns):
                count += 1

        return count

    def _optimize_stocks_table(self) -> int:
        """优化 stocks 表索引"""
        count = 0

        indexes = [
            # is_monitoring 索引（筛选监控中的股票）
            ('idx_stocks_monitoring', ['is_monitoring']),
            # is_pinned 索引（筛选置顶股票）
            ('idx_stocks_pinned', ['is_pinned']),
        ]

        for name, columns in indexes:
            if self.create_index_if_not_exists('stocks', name, columns):
                count += 1

        return count

    def analyze_query_performance(self) -> dict[str, dict]:
        """
        分析表的查询性能

        Returns:
            dict: 每个表的统计信息
        """
        stats = {}

        tables = ['logs', 'stocks', 'stock_news', 'sentiment_analysis', 'screener_results']

        with self.engine.begin() as conn:
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    row_count = result.scalar()

                    # 获取索引信息
                    inspector = inspect(self.engine)
                    indexes = inspector.get_indexes(table)

                    stats[table] = {
                        'row_count': row_count,
                        'index_count': len(indexes),
                        'indexes': [idx['name'] for idx in indexes],
                    }
                except Exception as e:
                    logger.warning(f"Could not analyze table {table}: {e}")
                    stats[table] = {'error': str(e)}

        return stats


def main():
    """主函数"""
    logger.info("Starting database index optimization...")

    optimizer = DatabaseIndexOptimizer()

    # 显示当前状态
    logger.info("\n=== Current Database Status ===")
    stats = optimizer.analyze_query_performance()
    for table, info in stats.items():
        if 'error' not in info:
            logger.info(f"{table}: {info['row_count']} rows, {info['index_count']} indexes")

    # 创建优化索引
    logger.info("\n=== Creating Optimized Indexes ===")
    results = optimizer.optimize_all_tables()

    total_created = sum(results.values())
    logger.info(f"\nTotal indexes created: {total_created}")
    for table, count in results.items():
        if count > 0:
            logger.info(f"  {table}: {count} new indexes")

    # 显示优化后状态
    logger.info("\n=== Optimized Database Status ===")
    stats = optimizer.analyze_query_performance()
    for table, info in stats.items():
        if 'error' not in info:
            logger.info(f"{table}: {info['row_count']} rows, {info['index_count']} indexes")
            logger.info(f"  Indexes: {', '.join(info['indexes'])}")

    logger.info("\nOptimization complete!")


if __name__ == "__main__":
    main()
