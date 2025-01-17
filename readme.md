# dbframe - 数据库框架

dbframe 是一个用于简化数据库操作的Python框架，支持多种数据库的统一接口。

## 功能特性

- 统一的API接口操作多种数据库
- 支持DataFrame格式的数据操作
- 内置连接池和缓存机制
- 支持异步操作
- 完善的类型提示

## 支持的数据库

- ClickHouse
- HDFS
- MongoDB
- MySQL
- Parquet
- PostgreSQL
- SQLite
- SQL Server

## 安装

```bash
pip install dbframe
```

## 快速开始

```python
from dbframe import MySQLAPI

# 初始化MySQL连接
db_mysql = MySQLAPI(host='localhost', user='root', password='123456', database='test')

# 查询数据
df = db_mysql.read_df("test_table")

# 插入数据
db_mysql.save(df, "test_table")
```

## API文档

查看完整的API文档：[API文档链接]

## 贡献指南

欢迎贡献代码！请阅读[贡献指南](CONTRIBUTING.md)了解如何参与开发。

## 许可证

本项目采用MIT许可证，详情请查看[LICENSE](LICENSE)文件。