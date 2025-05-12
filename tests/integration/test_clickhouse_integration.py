import pytest
import pandas as pd
from storage.clickhouse_connector import ClickHouseConnector
from config.settings import settings


@pytest.mark.integration
class TestClickHouseIntegration:
    @pytest.fixture(scope="class")
    def ch_client(self):
        client = ClickHouseConnector()
        yield client
        # 清理测试表
        client.execute_query("DROP TABLE IF EXISTS test_table")
        client.execute_query("DROP TABLE IF EXISTS test_table_external")
        client.execute_query("DROP TABLE IF EXISTS test_table_mv")

    def test_table_creation(self, ch_client):
        # 测试表创建
        ch_client.create_external_table("test_table", {
            "fields": ["id", "name"],
            "types": ["UInt32", "String"]
        }, "test/path.csv")

        ch_client.create_materialized_table("test_table", {
            "fields": ["id", "name"],
            "types": ["UInt32", "String"]
        })

        ch_client.create_materialized_view("test_table_external", "test_table")

        # 验证表是否存在
        tables = ch_client.execute_query("SHOW TABLES")
        table_names = [t[0] for t in tables]

        assert "test_table_external" in table_names
        assert "test_table" in table_names
        assert "test_table_mv" in table_names

    def test_data_insert_and_query(self, ch_client):
        # 测试数据插入和查询
        ch_client.execute_query("""
                                CREATE TABLE IF NOT EXISTS test_data
                                (
                                    id
                                    UInt32,
                                    value
                                    Float32
                                ) ENGINE = Memory
                                """)

        # 插入数据
        ch_client.execute_query("INSERT INTO test_data VALUES", [
            (1, 10.5),
            (2, 20.3),
            (3, 30.1)
        ])

        # 查询数据
        result = ch_client.execute_query("SELECT * FROM test_data ORDER BY id")

        assert len(result) == 3
        assert result[0] == (1, 10.5)
        assert result[1] == (2, 20.3)
        assert result[2] == (3, 30.1)