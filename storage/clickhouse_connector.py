from clickhouse_driver import Client
from config import config


class ClickHouseConnector:
    def __init__(self):
        # 连接设置使用 config 中的端口号
        self.client = Client(
            host=config.CH_HOST,  # 这里应该是 ClickHouse 服务的主机地址
            port=config.CH_PORT,  # 设置端口为 8014
            user=config.CH_USER,  # 用户名
            password=config.CH_PASSWORD,  # 密码
            **self._get_conn_options()  # 差异化连接配置
        )

    def _get_conn_options(self):
        # 根据环境区分连接选项
        return {
            'local': {'compression': False},
            'cloud': {'secure': True, 'ca_certs': '/etc/ssl/certs/ca-certificates.crt'}
        }[config.env]  # 根据环境选择不同配置

    def execute(self, query: str):
        """统一执行接口"""
        return self.client.execute(query)

    def initialize_tables(self):
        """初始化数据库表"""
        self.client.execute('''
        CREATE TABLE IF NOT EXISTS category (
            category_id UInt32,
            category_name String
        ) ENGINE = MergeTree()
        ORDER BY category_id;
        ''')

        self.client.execute('''
        CREATE TABLE IF NOT EXISTS price (
            item_id UInt32,
            category_id UInt32,
            date Date,
            price Float32
        ) ENGINE = MergeTree()
        ORDER BY (category_id, date);
        ''')
