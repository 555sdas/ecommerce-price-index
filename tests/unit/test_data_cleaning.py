import pytest
import pandas as pd
import numpy as np
from processing.data_cleaning import DataCleaner


class TestDataCleaner:
    @pytest.fixture
    def cleaner(self):
        return DataCleaner()

    @pytest.fixture
    def dirty_price_data(self):
        """提供包含各种问题的测试数据"""
        return pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04'],
            'item_id': ['A001', 'A002', 'A003', 'A004'],
            'category': ['食品', '家居', np.nan, '数码'],
            'price': [10.5, -5.0, 15.2, np.nan]
        })

    def test_clean_price_data(self, cleaner, dirty_price_data):
        """测试价格数据清洗功能"""
        clean_df, error = cleaner.clean_price_data(dirty_price_data)

        # 验证无错误
        assert error is None

        # 验证数据量
        assert len(clean_df) == 1  # 只保留完全有效的第1条

        # 验证保留的数据
        assert clean_df.iloc[0]['item_id'] == 'A001'
        assert clean_df.iloc[0]['category'] == '食品'
        assert clean_df.iloc[0]['price'] == 10.5

        # 验证过滤的数据
        assert 'A002' not in clean_df['item_id'].values  # 负价格
        assert 'A003' not in clean_df['item_id'].values  # 分类为NaN
        assert 'A004' not in clean_df['item_id'].values  # 价格为NaN

    def test_clean_all_invalid_data(self, cleaner):
        """测试全部无效数据的情况"""
        invalid_data = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'item_id': ['A001', 'A002'],
            'category': [np.nan, np.nan],
            'price': [-10.0, np.nan]
        })

        clean_df, error = cleaner.clean_price_data(invalid_data)

        assert clean_df is None
        assert error == "清洗后无有效数据"

    def test_clean_with_unit_conversion(self, cleaner):
        """测试包含单位转换的数据清洗"""
        test_data = pd.DataFrame({
            'date': ['2023-01-01', '2023-01-02'],
            'item_id': ['A001', 'A002'],
            'category': ['食品', '家居'],
            'price': [10.5, 20.0]
        })

        # 假设清洗类有单位转换功能
        cleaner.unit_conversion = True
        clean_df, error = cleaner.clean_price_data(test_data)

        assert error is None
        assert len(clean_df) == 2
        assert clean_df.iloc[0]['price'] == 1050  # 10.5 * 100
        assert clean_df.iloc[1]['price'] == 2000  # 20.0 * 100