import pandas as pd
from typing import List
from config.constants import PRICE_SCHEMA


class DataTransformer:
    def transform_price_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换价格数据格式"""
        # 确保日期格式统一
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 统一商品ID格式
        df["item_id"] = df["item_id"].astype(str).str.upper()

        # 统一分类名称
        df["category"] = df["category"].astype(str).str.strip()

        # 价格单位转换 (假设原始数据是元，转换为分)
        df["price"] = (df["price"] * 100).round().astype(int)

        return df[PRICE_SCHEMA["fields"]]

    def transform_category_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """转换分类数据格式"""
        # 统一分类名称
        df["name"] = df["name"].astype(str).str.strip()

        # 确保权重总和为1
        total_weight = df["weight"].sum()
        if total_weight != 1.0:
            df["weight"] = df["weight"] / total_weight

        return df