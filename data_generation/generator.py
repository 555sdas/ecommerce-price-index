import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple
from config.settings import settings
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA
import logging

logger = logging.getLogger("data_generator")


class DataGenerator:
    def __init__(self):
        np.random.seed(42)
        logger.info(f"Initializing DataGenerator with settings: "
                    f"SIMULATE_DAYS={settings.SIMULATE_DAYS}, "
                    f"CATEGORIES={settings.SIMULATE_CATEGORIES}, "
                    f"ITEMS_PER_CAT={settings.SIMULATE_ITEMS_PER_CAT}")

    def generate_category_data(self) -> pd.DataFrame:
        """生成商品分类数据"""
        logger.info("Generating category data...")
        categories = [
            {
                "category_id": i,
                "name": cat,
                "weight": settings.CATEGORY_WEIGHTS[cat]
            }
            for i, cat in enumerate(settings.SIMULATE_CATEGORIES)
        ]
        df = pd.DataFrame(categories, columns=CATEGORY_SCHEMA["fields"])
        logger.info(f"Generated category data with {len(df)} rows")
        return df

    def generate_price_data(self) -> pd.DataFrame:
        """生成商品价格数据"""
        logger.info("Generating price data...")
        # 生成日期范围
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=settings.SIMULATE_DAYS - 1)
        dates = pd.date_range(start=start_date, end=end_date)
        logger.info(f"Date range: {start_date} to {end_date}")

        # 生成商品基础数据
        items = []
        for cat in settings.SIMULATE_CATEGORIES:
            for i in range(settings.SIMULATE_ITEMS_PER_CAT):
                items.append({
                    'item_id': f"{cat[:1]}{i:03d}",
                    'category': cat,
                    'base_price': np.random.uniform(*settings.SIMULATE_PRICE_RANGE)
                })
        logger.info(f"Generated {len(items)} items")

        # 生成每日价格数据
        data = []
        for date in dates:
            for item in items:
                change_pct = np.random.normal(loc=0, scale=0.03)
                new_price = max(1.0, item['base_price'] * (1 + change_pct))
                data.append([date.date(), item['item_id'], item['category'], round(new_price, 2)])
                item['base_price'] = new_price

        df = pd.DataFrame(data, columns=PRICE_SCHEMA["fields"])
        logger.info(f"Generated price data with {len(df)} rows")
        return df

    def generate_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """生成所有数据，并追加写入 CSV 文件（含时间戳）"""
        logger.info("Starting to generate all data...")

        # 确保数据目录存在
        os.makedirs(settings.DATA_DIR, exist_ok=True)
        logger.info(f"Data directory: {settings.DATA_DIR}")

        category_df = self.generate_category_data()
        price_df = self.generate_price_data()

        # 添加时间戳列
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        category_df["timestamp"] = timestamp
        price_df["timestamp"] = timestamp

        # 设置备份路径
        category_path = settings.DATA_DIR / "category_backup.csv"
        price_path = settings.DATA_DIR / "price_backup.csv"
        logger.info(f"Backup paths - Category: {category_path}, Price: {price_path}")

        # 追加保存数据（若文件不存在则写入表头）
        try:
            category_df.to_csv(
                category_path, mode='a',
                header=not os.path.exists(category_path),
                index=False
            )
            price_df.to_csv(
                price_path, mode='a',
                header=not os.path.exists(price_path),
                index=False
            )
            logger.info("Data successfully saved to local CSV files")
        except Exception as e:
            logger.error(f"Failed to save data to CSV: {str(e)}")
            raise

        return category_df, price_df