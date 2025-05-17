from clickhouse_driver import Client

CH_HOST = "cc-bp143310x5229s4k4.public.clickhouse.ads.aliyuncs.com"
CH_PORT = 3306
CH_USER = "root1"
CH_PASSWORD = "Ea907e10dc8b"

def test_connection():
    try:
        client = Client(
            host=CH_HOST,
            port=CH_PORT,
            user=CH_USER,
            password=CH_PASSWORD
        )
        result = client.execute('SELECT version()')
        print("ClickHouse 连接成功，版本号：", result)
    except Exception as e:
        print("连接失败，错误信息：", e)

if __name__ == "__main__":
    test_connection()
