import pytest
from processing.schema_mapping import SchemaMapper
from config.constants import CATEGORY_SCHEMA, PRICE_SCHEMA


class TestSchemaMapper:
    @pytest.fixture
    def mapper(self):
        return SchemaMapper()

    def test_map_category_schema(self, mapper):
        test_data = {
            'category_id': '1',
            'name': '食品',
            'weight': '0.4',
            'extra_field': 'ignore_me'
        }

        mapped = mapper.map_category_schema(test_data)

        assert mapped == {
            'category_id': 1,
            'name': '食品',
            'weight': 0.4
        }

    def test_map_price_schema(self, mapper):
        test_data = {
            'date': '2023-01-01',
            'item_id': 'A001',
            'category': '食品',
            'price': '10.5'
        }

        mapped = mapper.map_price_schema(test_data)

        assert mapped == {
            'date': '2023-01-01',
            'item_id': 'A001',
            'category': '食品',
            'price': 10.5
        }