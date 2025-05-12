import pytest
import os
from config.settings import settings


def pytest_configure(config):
    # 测试专用配置
    settings.OSS_BUCKET_NAME = "test-bucket"
    settings.SIMULATE_DAYS = 7  # 测试时减少数据量
    settings.SIMULATE_ITEMS_PER_CAT = 5

    # 确保测试目录存在
    if not os.path.exists("tests/fixtures"):
        os.makedirs("tests/fixtures")


@pytest.fixture
def sample_category_data():
    return pd.DataFrame({
        'category_id': [1, 2, 3],
        'name': ['食品', '家居', '数码'],
        'weight': [0.4, 0.3, 0.3]
    })


@pytest.fixture
def sample_price_data():
    return pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'item_id': ['A001', 'A002', 'A003'],
        'category': ['食品', '家居', '数码'],
        'price': [10.5, 20.3, 15.2]
    })