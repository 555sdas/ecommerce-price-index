import os

# 定义目录和文件结构
structure = {
    'tests': {
        'unit': ['test_data_generation.py', 'test_data_cleaning.py', 'test_schema_mapping.py', 'test_oss_connector.py'],
        'integration': ['test_clickhouse_integration.py', 'test_full_pipeline.py'],
        'performance': ['test_data_volume.py', 'test_query_performance.py'],
        'fixtures': ['sample_category.csv', 'sample_price.csv', 'test_config.py'],
        'conftest.py': '',
        '__init__.py': '',
        '01.py': '''
import pytest

def test_example():
    assert 1 + 1 == 2
'''
    }
}

# 创建目录和文件
def create_structure(base_path, structure):
    for name, value in structure.items():
        current_path = os.path.join(base_path, name)
        if isinstance(value, dict):  # 如果是目录，递归创建
            os.makedirs(current_path, exist_ok=True)
            create_structure(current_path, value)
        elif isinstance(value, list):  # 如果是文件列表，创建文件
            for file_name in value:
                file_path = os.path.join(current_path, file_name)
                os.makedirs(os.path.dirname(file_path), exist_ok=True)  # 确保文件所在的目录存在
                with open(file_path, 'w') as f:
                    f.write('')
        elif isinstance(value, str):  # 如果是文件内容
            os.makedirs(os.path.dirname(current_path), exist_ok=True)  # 确保文件所在的目录存在
            with open(current_path, 'w') as f:
                f.write(value)

# 调用函数
create_structure('..', structure)
