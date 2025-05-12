import pandas as pd
from io import StringIO
from config import config  # 从config模块导入config对象

if config.is_local:
    from minio import Minio
else:
    import oss2


class OSSConnector:
    def __init__(self):
        if config.is_local:
            # MinIO本地配置
            self.client = Minio(
                config.MINIO_ENDPOINT,
                access_key=config.MINIO_ACCESS_KEY,
                secret_key=config.MINIO_SECRET_KEY,
                secure=False
            )
        else:
            # 阿里云OSS配置
            self.auth = oss2.Auth(
                config.OSS_ACCESS_KEY,  # 使用config对象而不是settings
                config.OSS_SECRET_KEY
            )
            self.bucket = oss2.Bucket(
                self.auth,
                config.OSS_ENDPOINT,
                config.OSS_BUCKET
            )

    def upload_dataframe(self, df, file_key: str) -> bool:
        """通用DataFrame上传方法"""
        try:
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            if config.is_local:
                from io import BytesIO
                self.client.put_object(
                    config.OSS_BUCKET,
                    file_key,
                    BytesIO(csv_buffer.getvalue().encode()),
                    length=len(csv_buffer.getvalue())
                )
            else:
                self.bucket.put_object(file_key, csv_buffer.getvalue())

            return True
        except Exception as e:
            print(f"上传失败: {str(e)}")
            return False