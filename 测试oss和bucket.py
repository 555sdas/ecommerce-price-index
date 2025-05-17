import os
import oss2
from datetime import datetime

def test_oss_connection():
    """测试OSS连接和Bucket权限"""
    try:
        # 从环境变量读取配置（必须提前设置）
        auth = oss2.Auth(
            os.getenv("OSS_ACCESS_KEY_ID"),
            os.getenv("OSS_ACCESS_KEY_SECRET")
        )
        bucket = oss2.Bucket(
            auth,
            "oss-cn-hangzhou.aliyuncs.com",  # 替换为你的Endpoint
            "prod-ecommerce-data"            # 替换为你的Bucket名称
        )

        # 测试1：检查Bucket是否存在
        exists = bucket.bucket_exists()
        print(f"✅ Bucket存在检查: {exists}")

        # 测试2：上传测试文件
        test_file = f"test_{datetime.now().strftime('%Y%m%d%H%M%S')}.txt"
        bucket.put_object(test_file, b"OSS connection test data")
        print(f"✅ 文件上传成功: {test_file}")

        # 测试3：列出文件（最多3个）
        objects = [obj.key for obj in oss2.ObjectIterator(bucket, max_keys=3)]
        print(f"✅ 文件列表前3项: {objects}")

        # 清理测试文件
        bucket.delete_object(test_file)
        print(f"✅ 已清理测试文件: {test_file}")

        return True

    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        return False

if __name__ == "__main__":
    print("===== OSS连接测试开始 =====")
    if not all([os.getenv("OSS_AK"), os.getenv("OSS_SK")]):
        print("❌ 请先设置环境变量 OSS_AK 和 OSS_SK")
    else:
        test_oss_connection()