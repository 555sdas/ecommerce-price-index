from pathlib import Path

class LocalConfig:
    SIMULATE_DAYS = 30
    SIMULATE_CATEGORIES = ['食品', '家居', '数码', '服饰']
    SIMULATE_ITEMS_PER_CAT = 50
    SIMULATE_PRICE_RANGE = (10, 100)
    CATEGORY_WEIGHTS = {'食品': 0.4, '家居': 0.2, '数码': 0.2, '服饰': 0.2}

    DATA_DIR = Path(__file__).parent.parent / "data"

    MINIO_ENDPOINT = "localhost:9003"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    OSS_BUCKET = "ecommerce-data"

    CH_HOST = "localhost"
    CH_PORT = 9004
    CH_USER = "root"
    CH_PASSWORD = "ea907"
    CH_CONNECT_ARGS = {'compression': False, 'settings': {'use_numpy': True}}

settings = LocalConfig()
