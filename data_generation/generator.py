import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple
from config.local_settings import settings
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA
import logging

logger = logging.getLogger("data_generator")
logger.setLevel(logging.INFO)

class DataGenerator:
    def __init__(self):
        np.random.seed(42)
        self.categories = settings.SIMULATE_CATEGORIES
        self.n_items = settings.SIMULATE_ITEMS_PER_CAT
        self.price_range = settings.SIMULATE_PRICE_RANGE
        self.days = pd.date_range(end=datetime.now().date(), periods=settings.SIMULATE_DAYS)

    def generate_category_data(self) -> pd.DataFrame:
        logger.info("Generating category data...")
        categories = [{
            "category_id": idx,
            "name": cat,
            "weight": settings.CATEGORY_WEIGHTS.get(cat, 0.0),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } for idx, cat in enumerate(self.categories)]
        return pd.DataFrame(categories, columns=["category_id", "name", "weight", "timestamp"])

    def generate_item_data(self) -> pd.DataFrame:
        logger.info("Generating item data...")
        item_records = []
        for cat_id, cat in enumerate(self.categories):
            for i in range(self.n_items):
                item_id = f"{cat[:1]}{i:03d}"
                item_records.append({
                    "item_id": item_id,
                    "category_id": cat_id
                })
        return pd.DataFrame(item_records, columns=["item_id", "category_id"])

    def generate_price_data(self, item_df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Generating price data...")
        base_prices = {item_id: np.random.uniform(*self.price_range) for item_id in item_df["item_id"]}
        data = []

        for date in self.days:
            for item_id in item_df["item_id"]:
                pct_change = np.random.normal(loc=0, scale=0.03)
                new_price = max(1.0, base_prices[item_id] * (1 + pct_change))
                data.append([
                    date.date(), item_id, round(new_price, 2)
                ])
                base_prices[item_id] = new_price

        df = pd.DataFrame(data, columns=["date", "item_id", "price"])
        return df

    def save_data(self, df: pd.DataFrame, filename: str):
        path = settings.DATA_DIR / filename
        df.to_csv(path, index=False)
        logger.info(f"Saved {filename} to: {path.resolve()}")

    def generate_all_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info("Generating all data...")
        settings.DATA_DIR.mkdir(parents=True, exist_ok=True)

        category_df = self.generate_category_data()
        item_df = self.generate_item_data()
        price_df = self.generate_price_data(item_df)


        self.save_data(category_df, "category.csv")
        self.save_data(item_df, "item.csv")
        self.save_data(price_df, "price.csv")

        return category_df, price_df, item_df
