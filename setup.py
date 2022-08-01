from setuptools import setup, find_packages

setup(
    name="dbframe",
    version="1.0",
    author="wujiede",
    author_email="547054738@qq.com",
    description="易于操作 dataFrame 的数据库助手",

    # 你要安装的包，通过 setuptools.find_packages 找到当前目录下有哪些包
    packages=find_packages(),
    install_requires=['pymongo', 'lz4', 'clickhouse_driver', 'sqlalchemy-clickhouse'],
    dependency_links=[
        'https://pypi.tuna.tsinghua.edu.cn/simple',
    ],
)
