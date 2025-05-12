import pytest
import time
from data_generation.generator import DataGenerator
from storage.oss_connector import OSSConnector


@pytest.mark.performance
class TestDataVolume:
    @pytest.fixture(scope="class")
    def large_dataset(self):
        generator = DataGenerator()
        # 生成大数据集 (10倍正常数据量)
        orig_days = generator.settings.SIMULATE_DAYS
        generator.settings.SIMULATE_DAYS = 300  # 10个月数据
        df = generator.generate_price_data()
        generator.settings.SIMULATE_DAYS = orig_days
        return df

    def test_large_data_generation(self, large_dataset):
        start_time = time.time()
        df = large_dataset
        generation_time = time.time() - start_time

        assert len(df) > 100000  # 确保生成了大量数据
        print(f"\nGenerated {len(df):,} rows in {generation_time:.2f} seconds")

        # 性能基准 (可根据实际硬件调整)
        assert generation_time < 30.0  # 30秒内生成10万+行数据

    def test_large_data_upload(self, large_dataset):
        oss = OSSConnector()

        start_time = time.time()
        success = oss.upload_dataframe(large_dataset, "performance_test/large_price.csv")
        upload_time = time.time() - start_time

        assert success is True
        print(f"\nUploaded {len(large_dataset):,} rows in {upload_time:.2f} seconds")

        # 性能基准
        assert upload_time < 60.0  # 1分钟内上传完成