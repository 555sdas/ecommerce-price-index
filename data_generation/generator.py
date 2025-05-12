import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple
from config.settings import settings
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA


class DataGenerator:
    def __init__(self):
        np.random.seed(42)

    def generate_category_data(self) -> pd.DataFrame:
        """生成商品分类数据"""
        categories = [
            {
                "category_id": i,
                "name": cat,
                "weight": settings.CATEGORY_WEIGHTS[cat]
            }
            for i, cat in enumerate(settings.SIMULATE_CATEGORIES)
        ]
        return pd.DataFrame(categories, columns=CATEGORY_SCHEMA["fields"])

    def generate_price_data(self) -> pd.DataFrame:
        """生成商品价格数据"""
        # 生成日期范围
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=settings.SIMULATE_DAYS - 1)
        dates = pd.date_range(start=start_date, end=end_date)

        # 生成商品基础数据
        items = []
        for cat in settings.SIMULATE_CATEGORIES:
            for i in range(settings.SIMULATE_ITEMS_PER_CAT):
                items.append({
                    'item_id': f"{cat[:1]}{i:03d}",
                    'category': cat,
                    'base_price': np.random.uniform(*settings.SIMULATE_PRICE_RANGE)
                })

        # 生成每日价格数据
        data = []
        for date in dates:
            for item in items:
                change_pct = np.random.normal(loc=0, scale=0.03)
                new_price = max(1.0, item['base_price'] * (1 + change_pct))
                data.append([date.date(), item['item_id'], item['category'], round(new_price, 2)])
                item['base_price'] = new_price

        return pd.DataFrame(data, columns=PRICE_SCHEMA["fields"])

    def generate_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """生成所有数据，并追加写入 CSV 文件（含时间戳）"""
        category_df = self.generate_category_data()
        price_df = self.generate_price_data()

        # 添加时间戳列
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        category_df["timestamp"] = timestamp
        price_df["timestamp"] = timestamp

        # 设置备份路径
        category_path = settings.DATA_DIR / "category_backup.csv"
        price_path = settings.DATA_DIR / "price_backup.csv"

        # 追加保存数据（若文件不存在则写入表头）
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

        return category_df, price_df
