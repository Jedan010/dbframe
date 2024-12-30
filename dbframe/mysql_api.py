import pandas as pd
from sqlalchemy.engine.url import URL

from dbframe.database_api import SQLDB


class MysqlDB(SQLDB):
    """
    Mysql 数据库
    """

    DEFAULT_PORT: int = 3306

    def __init__(
        self,
        url: URL = None,
        host: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        query: dict = {"charset": "utf8"},
    ):
        super().__init__(
            drivername="mysql+pymysql",
            url=url,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            query=query,
        )

    def get_table_columns(self, table, schema=None):
        return pd.read_sql_query(f"desc {table}", self.engine, index_col="Field").index

    @property
    def tables(self):
        return pd.Index(self.execute_sql("SHOW TABLES").iloc[:, 0])
