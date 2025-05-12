import pytest
import pandas as pd
from main import main
from storage.clickhouse_connector import ClickHouseConnector
from analysis.price_index import PriceIndexCalculator


@pytest.mark.integration
@pytest.mark.slow
class TestFullPipeline:
    @pytest.fixture(scope="class")
    def run_pipeline(self):
        # 运行完整流程
        main()

        # 获取计算结果
        ch = ClickHouseConnector()
        calculator = PriceIndexCalculator(ch.client)
        return calculator.calculate_all_indices()

    def test_pipeline_output(self, run_pipeline):
        indices = run_pipeline

        # 验证输出结构
        assert set(indices.keys()) == {'cavallo', 'tmall', 'merged'}

        # 验证数据完整性
        assert len(indices['merged']) > 0
        assert not indices['merged'].isnull().values.any()

        # 验证指数计算逻辑
        cavallo = indices['cavallo']
        tmall = indices['tmall']

        assert cavallo['cavallo_index'].iloc[0] == pytest.approx(1.0, 0.1)
        assert tmall['tmall_index'].iloc[0] == pytest.approx(1.0, 0.1)

        # 验证指数随时间变化
        assert cavallo['cavallo_index'].iloc[-1] != pytest.approx(1.0, 0.1)
        assert tmall['tmall_index'].iloc[-1] != pytest.approx(1.0, 0.1)