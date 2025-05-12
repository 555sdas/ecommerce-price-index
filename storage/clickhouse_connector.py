from clickhouse_driver import Client
from config import config
import logging


class ClickHouseConnector:
    def __init__(self):
        self.logger = logging.getLogger('clickhouse_connector')
        # 使用 config 中的设置进行连接
        self.client = Client(
            host=config.CH_HOST,  # ClickHouse 服务的主机地址
            port=config.CH_PORT,  # 端口号
            user=config.CH_USER,  # 用户名
            password=config.CH_PASSWORD,  # 密码
            **self._get_conn_options()  # 根据环境选择连接选项
        )
        self.logger.info("ClickHouse连接已建立")

    def _get_conn_options(self):
        """根据环境区分连接选项"""
        return {
            'local': {'compression': False},
            'cloud': {'secure': True, 'ca_certs': '/etc/ssl/certs/ca-certificates.crt'}
        }[config.env]  # 根据 config 中的环境来选择连接选项

    def execute(self, query: str, params=None):
        """统一执行 SQL 查询接口"""
        try:
            self.logger.debug(f"Executing query: {query}")
            if params:
                # 使用参数化查询的正确方式
                result = self.client.execute(query, params)
            else:
                result = self.client.execute(query)
            return result
        except Exception as e:
            self.logger.error(f"Error executing query: {e}")
            raise

    def execute_query(self, query: str, params=None, return_dataframe: bool = False):
        """
        执行查询并返回格式化结果
        :param query: SQL查询语句
        :param params: 查询参数
        :param return_dataframe: 是否返回pandas DataFrame
        :return: 查询结果(列表或DataFrame)
        """
        try:
            self.logger.debug(f"Executing query: {query}")
            if params:
                result, columns = self.client.execute(query, params, with_column_types=True)
            else:
                result, columns = self.client.execute(query, with_column_types=True)

            if return_dataframe:
                import pandas as pd
                column_names = [col[0] for col in columns]
                return pd.DataFrame(result, columns=column_names)
            else:
                column_names = [col[0] for col in columns]
                return [dict(zip(column_names, row)) for row in result]

        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            raise

    def initialize_tables(self):
        """初始化数据库表格，创建 category、item 和 price 表"""
        try:
            # 创建 category 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS category
                                (
                                    category_id UInt32,
                                    name        String,
                                    weight      Float64,
                                    timestamp   DateTime
                                ) ENGINE = MergeTree()
                                      ORDER BY category_id;
                                ''')

            # 创建 item 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS item
                                (
                                    item_id     String,
                                    category_id UInt32
                                ) ENGINE = MergeTree()
                                      ORDER BY item_id;
                                ''')

            # 创建 price 表
            self.client.execute('''
                                CREATE TABLE IF NOT EXISTS price
                                (
                                    date    Date,
                                    item_id String,
                                    price   Float64
                                ) ENGINE = MergeTree()
                                      ORDER BY (date, item_id);
                                ''')

            self.logger.info("Tables initialized successfully.")
        except Exception as e:
            self.logger.error(f"Failed to initialize tables: {str(e)}")
            raise

    def insert_category(self, category_data):
        """批量插入类别数据"""
        try:
            query = '''
                    INSERT INTO category (category_id, name, weight, timestamp)
                    VALUES \
                    '''
            # 直接使用execute方法插入数据
            self.client.execute(query, category_data)
            self.logger.info(f"Inserted {len(category_data)} rows into category table.")
        except Exception as e:
            self.logger.error(f"Failed to insert category data: {str(e)}")
            raise

    def insert_item(self, item_data):
        """批量插入商品数据"""
        try:
            query = '''
                    INSERT INTO item (item_id, category_id)
                    VALUES \
                    '''
            self.client.execute(query, item_data)
            self.logger.info(f"Inserted {len(item_data)} rows into item table.")
        except Exception as e:
            self.logger.error(f"Failed to insert item data: {str(e)}")
            raise

    def insert_price(self, price_data):
        """批量插入价格数据"""
        try:
            query = '''
                    INSERT INTO price (date, item_id, price)
                    VALUES \
                    '''
            self.client.execute(query, price_data)
            self.logger.info(f"Inserted {len(price_data)} rows into price table.")
        except Exception as e:
            self.logger.error(f"Failed to insert price data: {str(e)}")
            raise

    def close(self):
        """关闭连接"""
        try:
            self.client.disconnect()
            self.logger.info("ClickHouse连接已关闭")
        except Exception as e:
            self.logger.error(f"Error closing connection: {str(e)}")