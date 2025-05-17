import os
import warnings
from typing import Dict, Any


class CloudConfig:
    """配置类（生产环境建议使用KMS或Vault管理敏感信息）"""

    # ============ 数据生成配置 ============
    SIMULATE_DAYS = 30
    SIMULATE_CATEGORIES = ['食品', '家居', '数码', '服饰']
    SIMULATE_ITEMS_PER_CAT = 50
    SIMULATE_PRICE_RANGE = (10, 100)
    CATEGORY_WEIGHTS = {'食品': 0.4, '家居': 0.2, '数码': 0.2, '服饰': 0.2}

    # ============ OSS配置 ============
    OSS_ENDPOINT = "oss-cn-hangzhou.aliyuncs.com"
    OSS_BUCKET = "prod-ecommerce-data"

    # 从环境变量读取（变量名建议大写）
    OSS_ACCESS_KEY = os.getenv("OSS_ACCESS_KEY_ID")  # 标准命名
    OSS_SECRET_KEY = os.getenv("OSS_ACCESS_KEY_SECRET")

    # ============ ClickHouse配置 ============
    CH_HOST = "cc-bp143310x5229s4k4.public.clickhouse.ads.aliyuncs.com"
    CH_PORT = 3306
    CH_USER = os.getenv("CLICKHOUSE_USER", "default")  # 带默认值
    CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")  # 必须通过环境变量注入

    # 连接参数（增强安全性）
    CH_CONNECT_ARGS: Dict[str, Any] = {
        'secure': True,
        'verify': True,  # 生产环境必须验证证书
        'ca_certs': '/etc/ssl/certs/ca-certificates.crt',
        'settings': {
            'use_numpy': True,
            'connect_timeout': 10,
            'max_execution_time': 30
        }
    }

    @classmethod
    def validate(cls):
        """启动时验证关键配置"""
        if not all([cls.OSS_ACCESS_KEY, cls.OSS_SECRET_KEY]):
            raise ValueError("OSS AccessKey/SecretKey 未设置！请检查环境变量")
        if not cls.CH_PASSWORD:
            raise ValueError("ClickHouse 密码不能为空！")
        if ":3306" not in cls.CH_HOST:
            warnings.warn("检测到非标准端口配置，请确认连接安全性", UserWarning)


# 初始化时自动验证
settings = CloudConfig()
CloudConfig.validate()