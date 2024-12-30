import pandas as pd
from sqlalchemy.engine.url import URL

from dbframe.database_api import SQLDB


class PostgreSQLDB(SQLDB):
    """
    PostgreSQL 数据库
    """

    DEFAULT_PORT: int = 5432

    def __init__(
        self,
        url: URL = None,
        host: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        query: dict = None,
    ) -> None:
        super().__init__(
            drivername="postgresql+psycopg2",
            url=url,
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            query=query,
        )

    def get_table_columns(self, table: str, schema: str = None) -> pd.Index:
        """取得表的列名"""
        if schema is None and "." in table:
            schema, table = table.split(".")
        query = [("table_name", table)]
        if schema is not None:
            query.append(("table_schema", schema))
        sql = self._gen_sql(
            table="information_schema.columns", fields="column_name", query=query
        )
        return self.execute_sql(sql, index_col="column_name").index

    @property
    def databases(self) -> list[str]:
        return self.execute_sql("SELECT datname FROM pg_database")["datname"].tolist()

    @property
    def schemas(self) -> list[str]:
        return self.execute_sql("SELECT schema_name FROM information_schema.schemata")[
            "schema_name"
        ].tolist()

    def get_tables(self, schema: str) -> list[str]:
        """取得 schema 下的所有表名"""
        return self.execute_sql(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema='{schema}'",
        )["table_name"].tolist()

    @property
    def tables(self) -> dict[str, list[str]]:
        return {s: self.get_tables(s) for s in self.schemas}
