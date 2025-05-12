# storage/oss_connector.py
import oss2
from io import StringIO


class OSSConnector:
    def __init__(self, endpoint=None, access_key_id=None, access_key_secret=None, bucket_name=None):
        """
        初始化OSS连接器

        参数:
            endpoint: OSS端点
            access_key_id: 访问密钥ID
            access_key_secret: 访问密钥
            bucket_name: 存储桶名称
        """
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.access_key_secret = access_key_secret
        self.bucket_name = bucket_name
        self._bucket = None

    @property
    def bucket(self):
        """延迟初始化bucket"""
        if self._bucket is None:
            auth = oss2.Auth(self.access_key_id, self.access_key_secret)
            self._bucket = oss2.Bucket(auth, self.endpoint, self.bucket_name)
        return self._bucket

    def upload_dataframe(self, df, object_name):
        """上传DataFrame到OSS"""
        try:
            # 直接使用to_csv返回的字符串
            csv_data = df.to_csv(index=False)
            self.bucket.put_object(object_name, csv_data)
            return True
        except Exception as e:
            print(f"上传到OSS失败: {str(e)}")
            return False

    def upload_category_data(self, df) -> bool:
        """上传分类数据"""
        return self.upload_dataframe(df, OSS_FILE_PATHS["category"])

    def upload_price_data(self, df) -> bool:
        """上传价格数据"""
        return self.upload_dataframe(df, OSS_FILE_PATHS["price"])

    def list_objects(self, prefix: str = "") -> list:
        """列出OSS中的对象"""
        try:
            return [obj.key for obj in oss2.ObjectIterator(self.bucket, prefix=prefix)]
        except Exception as e:
            print(f"列出OSS对象失败: {e}")
            return []