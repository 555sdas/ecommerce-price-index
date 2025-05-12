from data_generation.generator import DataGenerator
from data_generation.validator import DataValidator
from storage.oss_connector import OSSConnector
from storage.clickhouse_connector import ClickHouseConnector
from processing.data_cleaning import DataCleaner
from processing.transformer import DataTransformer
from analysis.price_index import PriceIndexCalculator
from analysis.visualization import PriceIndexVisualizer
from config.settings import settings
import pandas as pd
import logging


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def main():
    setup_logging()
    logger = logging.getLogger("main")

    try:
        logger.info("1. 生成模拟数据...")
        generator = DataGenerator()
        category_df, price_df = generator.generate_all_data()

        logger.info("2. 验证数据有效性...")
        validator = DataValidator()
        valid, msg = validator.validate_category_data(category_df)
        if not valid:
            raise ValueError(f"分类数据验证失败: {msg}")

        valid, msg = validator.validate_price_data(price_df)
        if not valid:
            raise ValueError(f"价格数据验证失败: {msg}")

        logger.info("3. 数据清洗与转换...")
        cleaner = DataCleaner()
        transformer = DataTransformer()

        category_df, _ = cleaner.clean_category_data(category_df)
        price_df, _ = cleaner.clean_price_data(price_df)

        category_df = transformer.transform_category_data(category_df)
        price_df = transformer.transform_price_data(price_df)

        logger.info("4. 上传数据到OSS...")
        oss = OSSConnector()
        if not oss.upload_category_data(category_df):
            raise RuntimeError("分类数据上传OSS失败")
        if not oss.upload_price_data(price_df):
            raise RuntimeError("价格数据上传OSS失败")

        logger.info("5. 初始化ClickHouse表结构...")
        ch = ClickHouseConnector()
        ch.initialize_tables()

        logger.info("6. 计算价格指数...")
        calculator = PriceIndexCalculator(ch.client)
        indices = calculator.calculate_all_indices()

        logger.info("7. 可视化结果...")
        visualizer = PriceIndexVisualizer()
        visualizer.plot_indices(indices, settings.DATA_DIR / "price_indices.png")

        logger.info("处理完成!")

    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)


if __name__ == "__main__":
    main()