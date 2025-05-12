import numpy as np
import pandas as pd
from typing import Dict, List
from clickhouse_driver import Client
from config.settings import settings


class PriceIndexCalculator:
    def __init__(self, ch_client: Client):
        self.client = ch_client

    def calculate_cavallo_index(self) -> pd.DataFrame:
        """计算Cavallo指数"""
        query = """
                WITH price_changes AS (SELECT
                    date \
                   , item_id \
                   , category \
                   , price / any (price) OVER (PARTITION BY item_id ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS price_ratio
                FROM price
                WHERE isNotNull(price_ratio) \
                  AND price_ratio \
                    > 0
                    ) \
                    , category_geom_avg AS (
                SELECT
                    date, category, exp(avg (log(price_ratio))) AS geom_avg_ratio
                FROM price_changes
                GROUP BY date, category
                    )
                SELECT
                    date, sum (geom_avg_ratio * c.weight) AS cavallo_index
                FROM category_geom_avg
                    JOIN category c \
                ON category_geom_avg.category = c.name
                GROUP BY date
                ORDER BY date \
                """
        return self.client.query_dataframe(query)

    def calculate_tmall_index(self) -> pd.DataFrame:
        """计算Tmall指数"""
        query = """
                WITH daily_avg_ratio AS (SELECT
                    date \
                   , avg (price / any (price) OVER (PARTITION BY item_id ORDER BY date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)) AS avg_ratio
                FROM price
                WHERE isNotNull(avg_ratio) AND avg_ratio > 0
                GROUP BY date
                    )
                SELECT
                    date, exp(sum (log(avg_ratio)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS tmall_index
                FROM daily_avg_ratio
                ORDER BY date \
                """
        return self.client.query_dataframe(query)

    def calculate_all_indices(self) -> Dict[str, pd.DataFrame]:
        """计算所有指数"""
        cavallo = self.calculate_cavallo_index()
        tmall = self.calculate_tmall_index()

        # 合并结果
        merged = pd.merge(cavallo, tmall, on="date")

        # 保存到ClickHouse
        self.client.execute(
            "INSERT INTO price_index VALUES",
            [(row.date, row.cavallo_index, row.tmall_index)
             for row in merged.itertuples()]
        )

        return {
            "cavallo": cavallo,
            "tmall": tmall,
            "merged": merged
        }