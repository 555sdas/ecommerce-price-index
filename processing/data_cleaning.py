import pandas as pd
from typing import Tuple, Optional
from config.constants import PRICE_SCHEMA


class DataCleaner:
    def __init__(self, unit_conversion=False):
        self.unit_conversion = unit_conversion  # 是否进行单位转换

    def clean_price_data(self, df):
        """清洗价格数据"""
        clean_df = df.copy()

        # 1. 过滤无效分类
        clean_df = clean_df[clean_df['category'].notna()]

        # 2. 过滤无效价格
        clean_df = clean_df[(clean_df['price'] > 0) & (clean_df['price'].notna())]

        # 3. 单位转换(如果需要)
        if self.unit_conversion:
            clean_df['price'] = clean_df['price'] * 100

        # 4. 重置索引
        clean_df = clean_df.reset_index(drop=True)

        if len(clean_df) == 0:
            return None, "清洗后无有效数据"

        return clean_df, None

    def clean_category_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
        """清洗分类数据"""
        # 处理空值
        df_clean = df.dropna()

        # 确保权重在0-1之间
        df_clean["weight"] = df_clean["weight"].clip(0, 1)

        return df_clean, None