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
        # 检查是否包含所有必要的列
        #print(f"DataFrame columns: {df.columns}")  # 调试输出
        missing_columns = [col for col in PRICE_SCHEMA["fields"] if col not in df.columns]
        if missing_columns:
            return False, f"缺少必要的列: {','.join(missing_columns)}"

        # 检查是否存在空值
        if df.isnull().values.any():
            return False, "存在空值"

        # 检查价格是否大于0
        if df['price'].min() <= 0:
            return False, "价格必须大于0"

        return True, None

    def validate_item_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """验证商品数据"""
        # 确保 item 数据包含必要的列
        if not all(col in df.columns for col in ["item_id", "category_id"]):
            return False, "缺少必要的列: item_id 或 category_id"

        if df.isnull().values.any():
            return False, "存在空值"

        return True, None
