import pandas as pd
from typing import Tuple, Optional
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA


class DataValidator:
    def validate_category_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """验证分类数据"""
        if not all(col in df.columns for col in CATEGORY_SCHEMA["fields"]):
            return False, "缺少必要的列"

        if df.isnull().values.any():
            return False, "存在空值"

        if df['weight'].min() <= 0 or df['weight'].max() > 1:
            return False, "权重值必须在0-1之间"

        return True, None

    def validate_price_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """验证价格数据"""
        if not all(col in df.columns for col in PRICE_SCHEMA["fields"]):
            return False, "缺少必要的列"

        if df.isnull().values.any():
            return False, "存在空值"

        if df['price'].min() <= 0:
            return False, "价格必须大于0"

        return True, None