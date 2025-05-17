import os
import oss2
from datetime import datetime

def test_oss_connection():
    """测试OSS连接和Bucket权限"""
    try:
        # 从环境变量读取配置
        access_key = os.getenv("OSS_ACCESS_KEY_ID")
        access_secret = os.getenv("OSS_ACCESS_KEY_SECRET")
        if not all([access_key, access_secret]):
            print("❌ 未设置环境变量 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET")
            return False

        auth = oss2.Auth(access_key, access_secret)
        bucket = oss2.Bucket(
            auth,
            "oss-cn-hangzhou.aliyuncs.com",  # 替换为你的 Endpoint
            "prod-ecommerce-data"            # 替换为你的 Bucket 名称
        )

        # ✅ 使用兼容方法检查 Bucket 是否存在
        try:
            bucket.get_bucket_info()
            print("✅ Bucket 存在检查通过")
        except oss2.exceptions.NoSuchBucket:
            print("❌ Bucket 不存在")
            return False

        # 上传测试文件
        test_file = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
        bucket.put_object(test_file, b"OSS connection test data")
        print(f"✅ 文件上传成功: {test_file}")

        # 列出前3个对象
        objects = [obj.key for obj in oss2.ObjectIterator(bucket, max_keys=3)]
        print(f"✅ 文件列表前3项: {objects}")

        # 删除测试文件
        bucket.delete_object(test_file)
        print(f"✅ 已清理测试文件: {test_file}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    print("===== OSS连接测试开始 =====")
    test_oss_connection()
