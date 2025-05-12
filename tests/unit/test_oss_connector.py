# tests/unit/test_oss_connector.py
import pytest
from unittest.mock import MagicMock, patch
from storage.oss_connector import OSSConnector


class TestOSSConnector:
    @pytest.fixture
    def oss_connector(self):
        """提供配置好的OSSConnector测试实例"""
        # 创建连接器实例
        connector = OSSConnector(
            endpoint="test-endpoint",
            access_key_id="test-key-id",
            access_key_secret="test-secret",
            bucket_name="test-bucket"
        )

        # Mock bucket属性
        connector._bucket = MagicMock()
        return connector

    def test_upload_dataframe_success(self, oss_connector):
        """测试成功上传DataFrame到OSS"""
        # 准备测试数据
        mock_df = MagicMock()
        mock_df.to_csv.return_value = "test,csv,data"

        # 执行上传
        result = oss_connector.upload_dataframe(mock_df, "test/path.csv")

        # 验证结果
        assert result is True
        mock_df.to_csv.assert_called_once_with(index=False)
        oss_connector.bucket.put_object.assert_called_once_with(
            "test/path.csv",
            "test,csv,data"
        )

    def test_upload_dataframe_failure(self, oss_connector):
        """测试上传失败情况"""
        # 准备测试数据
        mock_df = MagicMock()
        mock_df.to_csv.return_value = "test,csv,data"

        # 配置mock抛出异常
        oss_connector.bucket.put_object.side_effect = Exception("Upload failed")

        # 执行测试
        result = oss_connector.upload_dataframe(mock_df, "test/path.csv")

        # 验证结果
        assert result is False
        mock_df.to_csv.assert_called_once_with(index=False)
        oss_connector.bucket.put_object.assert_called_once_with(
            "test/path.csv",
            "test,csv,data"
        )

    def test_upload_empty_dataframe(self, oss_connector):
        """测试上传空DataFrame"""
        # 准备空DataFrame Mock
        mock_df = MagicMock()
        mock_df.to_csv.return_value = ""

        # 执行上传
        result = oss_connector.upload_dataframe(mock_df, "empty.csv")

        # 验证结果
        assert result is True
        mock_df.to_csv.assert_called_once_with(index=False)
        oss_connector.bucket.put_object.assert_called_once_with(
            "empty.csv",
            ""
        )