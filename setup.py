from setuptools import setup, find_packages

setup(
    name="dbframe",
    version="1.0",
    author="wujiede",
    author_email="547054738@qq.com",
    description="易于操作 dataFrame 的数据库助手",
    # 你要安装的包，通过 setuptools.find_packages 找到当前目录下有哪些包
    packages=find_packages(),
    install_requires=[
        "sqlalchemy",
        # "clickhouse_driver",
        # "clickhouse-sqlalchemy",
        # "pymysql",
        # "pymongo",
        # "lz4",
        # "pyarrow",
        # "fastparquet",
        # "psycopg2-binary",
    ],
)
