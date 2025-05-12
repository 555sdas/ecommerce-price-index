import pytest
import time
from storage.clickhouse_connector import ClickHouseConnector
from analysis.price_index import PriceIndexCalculator


@pytest.mark.performance
class TestQueryPerformance:
    @pytest.fixture(scope="class")
    def calculator(self):
        ch = ClickHouseConnector()
        return PriceIndexCalculator(ch.client)

    def test_cavallo_index_performance(self, calculator):
        start_time = time.time()
        result = calculator.calculate_cavallo_index()
        query_time = time.time() - start_time

        assert len(result) > 0
        print(f"\nCalculated Cavallo index ({len(result)} days) in {query_time:.2f} seconds")

        # 性能基准
        assert query_time < 5.0  # 5秒内完成计算

    def test_tmall_index_performance(self, calculator):
        start_time = time.time()
        result = calculator.calculate_tmall_index()
        query_time = time.time() - start_time

        assert len(result) > 0
        print(f"\nCalculated Tmall index ({len(result)} days) in {query_time:.2f} seconds")

        # 性能基准
        assert query_time < 3.0  # 3秒内完成计算