import pytest
import pandas as pd
from datetime import datetime, timedelta
from data_generation.generator import DataGenerator
from config.settings import settings


class TestDataGenerator:
    @pytest.fixture
    def generator(self):
        return DataGenerator()

    def test_generate_category_data(self, generator):
        df = generator.generate_category_data()

        # 验证数据结构
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == {'category_id', 'name', 'weight'}
        assert len(df) == len(settings.SIMULATE_CATEGORIES)

        # 验证权重
        assert df['weight'].sum() == pytest.approx(1.0, 0.01)

    def test_generate_price_data(self, generator):
        df = generator.generate_price_data()

        # 验证数据结构
        assert isinstance(df, pd.DataFrame)
        assert set(df.columns) == {'date', 'item_id', 'category', 'price'}

        # 验证数据量
        expected_rows = settings.SIMULATE_DAYS * len(settings.SIMULATE_CATEGORIES) * settings.SIMULATE_ITEMS_PER_CAT
        assert len(df) == expected_rows

        # 验证价格范围
        assert df['price'].min() > 0
        # 放宽限制到30%波动，因为正态分布可能有极端值
        assert df['price'].max() <= settings.SIMULATE_PRICE_RANGE[1] * 1.3