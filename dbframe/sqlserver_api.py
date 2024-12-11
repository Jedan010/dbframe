import pandas as pd
from sqlalchemy.engine.url import URL

from dbframe.database_api import SQLDB


class SqlSeverDB(SQLDB):
    DEFAULT_PORT: int = 1433

    def __init__(
        self,
        url: URL = None,
        host: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        query: dict = {"charset": "cp936"},
        drivername: str = "mssql+pymssql",
        **kwargs,
    ):
        super().__init__(
            url=url,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            query=query,
            drivername=drivername,
            **kwargs,
        )

    @property
    def database(self):
        """获取数据库名"""
        return self.execute_sql("SELECT name FROM sys.databases;")["name"].tolist()

    @property
    def tables(self):
        """获取数据库中的所有表"""
        return self.execute_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='dbo';"
        )["table_name"].tolist()

    def get_table_columns(self, table: str, schema: str = None):
        """获取表的列名"""
        sql = f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}'"
        if schema is not None:
            sql += f" AND table_schema='{schema}';"
        cols: pd.Index = self.execute_sql(sql, index_col="column_name").index
        return cols
