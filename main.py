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
        category_df, price_df, item_df = generator.generate_all_data()  # 更新，接收 item_df\



        logger.info("2. 验证数据有效性...")
        validator = DataValidator()
        valid, msg = validator.validate_category_data(category_df)
        if not valid:
            raise ValueError(f"分类数据验证失败: {msg}")

        valid, msg = validator.validate_price_data(price_df)
        if not valid:
            raise ValueError(f"价格数据验证失败: {msg}")

        valid, msg = validator.validate_item_data(item_df)  # 添加对 item 数据的验证
        if not valid:
            raise ValueError(f"商品数据验证失败: {msg}")

        logger.info("3. 数据清洗与转换...")
        cleaner = DataCleaner()
        # 在数据清洗后立即添加
        print("清洗后price列数据类型:", price_df['price'].dtype)
        print("价格样例（前5行）:\n", price_df['price'].head())
        print("异常价格统计:", price_df[price_df['price'] > 1000]['price'].describe())
        transformer = DataTransformer()

        category_df, _ = cleaner.clean_category_data(category_df)
        price_df, _ = cleaner.clean_price_data(price_df)
        item_df, _ = cleaner.clean_item_data(item_df)  # 添加对 item 数据的清洗

        category_df = transformer.transform_category_data(category_df)
        price_df = transformer.transform_price_data(price_df)
        item_df = transformer.transform_item_data(item_df)  # 添加对 item 数据的转换

        logger.info("4. 上传数据到OSS...")

        oss = OSSConnector()
        if not oss.upload_dataframe(category_df, "category/category.csv"):
            raise RuntimeError("分类数据上传OSS失败")

        if not oss.upload_dataframe(item_df, "item/item.csv"):  # 上传 item 数据
            raise RuntimeError("商品数据上传OSS失败")

        if not oss.upload_dataframe(price_df, "price/price.csv"):
            raise RuntimeError("价格数据上传OSS失败")

        logger.info("5. 初始化ClickHouse表结构...")
        ch = ClickHouseConnector()
        ch.initialize_tables()  # 初始化所有表格，确保 item 表也被创建
        # 在main.py的ClickHouse初始化后添加
        table_info = ch.execute_query("DESCRIBE TABLE price")
        print("Price表结构:", table_info)


        # 转换时间字段为 datetime 类型
        category_df['timestamp'] = pd.to_datetime(category_df['timestamp'])
        price_df['date'] = pd.to_datetime(price_df['date'])


        print("Price表列名:", price_df.columns.tolist())  # 应输出 ['Date', 'item_id', 'price']


        # 准备数据格式（注意要用 .values.tolist() 或 .itertuples()）
        ch.insert_category(category_df[['category_id', 'name', 'weight', 'timestamp']].values.tolist())
        ch.insert_item(item_df[['item_id', 'category_id']].values.tolist())
        ch.insert_price(price_df[['date', 'item_id', 'price']].values.tolist())
        logger.info("验证基础数据...")

        print("Category columns:", category_df.columns)  # 应包含 category_id, name, weight
        print("Item columns:", item_df.columns)  # 应包含 item_id, category_id
        print("Price columns:", price_df.columns)  # 应包含 date, item_id, price

        # 在主函数中添加更多调试信息
        logger.info("6. 计算价格指数...")
        calculator = PriceIndexCalculator(ch)

        # 计算Cavallo指数
        cavallo_indices = calculator.calculate_cavallo_index(base_mode='auto')
        logger.info(f"Cavallo指数数据量: {len(cavallo_indices)}")
        if cavallo_indices:
            logger.info(f"首条记录: {cavallo_indices[0]}")

        # 计算Tmall指数
        tmall_indices = calculator.calculate_tmall_index(base_mode='auto')
        logger.info(f"Tmall指数数据量: {len(tmall_indices)}")
        if tmall_indices:
            logger.info(f"首条记录: {tmall_indices[0]}")

        # 检查数据时间范围
        if cavallo_indices and tmall_indices:
            logger.info(f"日期s范围: {cavallo_indices[0]['date']} 至 {cavallo_indices[-1]['date']}")

        logger.info("7. 可视化结果...")
        try:
            visualizer = PriceIndexVisualizer()
            visualizer.visualize_all()
            logger.info("可视化完成!")
        except Exception as e:
            logger.error(f"可视化失败: {str(e)}", exc_info=True)

        logger.info("处理完成!")

    except Exception as e:
        logger.error(f"处理失败: {e}", exc_info=True)


if __name__ == "__main__":
    main()
