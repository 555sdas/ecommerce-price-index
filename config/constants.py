# 数据表结构定义
CATEGORY_SCHEMA = {
    "fields": ["category_id", "name", "weight"],
    "types": ["UInt32", "String", "Float32"]
}

PRICE_SCHEMA = {
    "fields": ["date", "item_id", "category", "price"],
    "types": ["Date", "String", "String", "Float32"]
}

# OSS文件路径
OSS_FILE_PATHS = {
    "category": "category/category.csv",
    "price": "price/price.csv"
}