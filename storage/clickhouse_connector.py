from clickhouse_driver import Client
from typing import List, Optional
from config.settings import settings
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA, OSS_FILE_PATHS


class ClickHouseConnector:
    def __init__(self):
        self.client = Client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            user=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD
        )

    def execute_query(self, query: str, params=None):
        """执行SQL查询"""
        return self.client.execute(query, params)

    def create_external_table(self, table_name: str, schema: dict, file_path: str):
        """创建OSS外部表"""
        fields_with_types = [
            f"{field} {type_}"
            for field, type_ in zip(schema["fields"], schema["types"])
        ]

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}_external (
            {', '.join(fields_with_types)}
        ) ENGINE = URL('https://{settings.OSS_BUCKET_NAME}.{settings.OSS_ENDPOINT}/{file_path}', 'CSV')
        """
        self.execute_query(query)

    def create_materialized_table(self, table_name: str, schema: dict):
        """创建物化表"""
        fields_with_types = [
            f"{field} {type_}"
            for field, type_ in zip(schema["fields"], schema["types"])
        ]

        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(fields_with_types)}
        ) ENGINE = MergeTree()
        ORDER BY {schema["fields"][0]}
        """
        self.execute_query(query)

    def create_materialized_view(self, source_table: str, target_table: str):
        """创建物化视图"""
        query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {target_table}_mv 
        TO {target_table} 
        AS SELECT * FROM {source_table}
        """
        self.execute_query(query)

    def initialize_tables(self):
        """初始化所有表结构"""
        # 分类数据
        self.create_external_table("category", CATEGORY_SCHEMA, OSS_FILE_PATHS["category"])
        self.create_materialized_table("category", CATEGORY_SCHEMA)
        self.create_materialized_view("category_external", "category")

        # 价格数据
        self.create_external_table("price", PRICE_SCHEMA, OSS_FILE_PATHS["price"])
        self.create_materialized_table("price", PRICE_SCHEMA)
        self.create_materialized_view("price_external", "price")

        # 价格指数表
        self.create_materialized_table("price_index", {
            "fields": ["date", "cavallo_index", "tmall_index"],
            "types": ["Date", "Float32", "Float32"]
        })