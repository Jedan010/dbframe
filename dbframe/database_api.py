from abc import ABC, abstractmethod
from copy import deepcopy

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url

from dbframe.cache import global_cache
from dbframe.utils import db_repet


class DatabaseTemplate(ABC):
    """数据库模板"""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}{self._params}"

    def __repr__(self) -> str:
        return str(self)

    @property
    def _params(self) -> tuple:
        """数据库参数"""
        return ()

    @property
    def tables(self) -> list[str]:
        """
        获取所有表
        """
        pass

    @abstractmethod
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fileds: list[str] = None,
        symbols: list[str] = None,
        query: list[str] = None,
        date_name: str = "date",
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        从数据库读取数据并转化为dataFrame格式
        """
        pass

    @abstractmethod
    def save_df(self, df: pd.DataFrame, table: str, **kwargs) -> bool:
        """
        保存 dataFrame 数据至数据库中
        """
        pass

    def _list2str(self, lst: list[str]):
        if isinstance(lst, (str, int, float, bool)):
            lst = [lst]
        return str(tuple(lst)).replace(",)", ")")

    def _format_query(self, query: list[str]) -> list[str]:
        if query is None:
            query = []
        elif isinstance(query, (str, dict, tuple)):
            query = [query]

        new_query: list[str] = []
        for x in query:
            if isinstance(x, str):
                new_query.append(x)
            elif isinstance(x, (list, tuple)):
                if len(x) == 1:
                    new_query.append(x[0])
                elif len(x) == 2:
                    if x[1] is None:
                        continue
                    new_query.append(f"{x[0]} in {self._list2str(x[1])}")
                elif len(x) == 3:
                    _other = x[2]
                    if isinstance(_other, (list, tuple, pd.Index)):
                        _other = self._list2str(_other)
                    new_query.append(f"{x[0]} {x[1]} {_other} ")
                else:
                    raise ValueError(f"query {x} format error")
            elif isinstance(x, dict):
                for k, v in x.items():
                    new_query.append(f"{k} in {self._list2str(v)}")
            else:
                raise ValueError(f"query {x} format error")

        return new_query

    def _gen_sql(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[str] = None,
        schema: str = None,
        date_name: str = "date",
        oper: str = "SELECT",
        other_sql: str = None,
        op_format: str = None,
    ) -> str:
        """
        生成 SQL 语句
        """
        query = self._format_query(query)
        if start:
            query.append(f"{date_name} >= '{start}'")
        if end:
            if date_name == "datetime":
                _end = pd.to_datetime(end)
                if _end.hour == _end.minute == _end.second == 0:
                    end = _end.to_period("D").end_time.replace(
                        microsecond=0, nanosecond=0
                    )
            query.append(f"{date_name} <= '{end}'")
        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols_str = self._list2str(symbols)
            query.append(f"symbol in {symbols_str}")

        query = [f"({x})" for x in query]
        where = "WHERE " + " AND ".join(query) if query else ""

        if fields is None:
            fields = "*"
        elif not isinstance(fields, str):
            fields = ", ".join([f'"{x}"' for x in fields])
        else:
            fields = f'"{fields}"'

        if oper in ["delete", "DELETE"]:
            fields = ""

        other_sql = other_sql if other_sql else ""
        op_format = f"FORMAT {op_format}" if op_format else ""

        if schema is not None:
            table = f"{schema}.{table}"

        SQL = f"""
            {oper} {fields}
            FROM {table}
            {where}
            {other_sql}
            {op_format}
            """

        return SQL

    def _set_url(url: URL, key: str, value: str):
        if sqlalchemy.__version__ < "1.4":
            setattr(url, key, value)
        else:
            url.set(key, value)


class SQLDB(DatabaseTemplate):
    DEFAULT_PORT: int

    def __init__(
        self,
        drivername: str,
        url: URL = None,
        host: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
        query: dict = None,
    ) -> None:
        if url is None:
            if host is None:
                host = "localhost"
            if port is None:
                port = self.DEFAULT_PORT
            if database is None:
                database = "default"
            if username is None:
                username = "root"
            if password is None:
                password = ""
            if query is None:
                query = {}

            url = URL(
                drivername=drivername,
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
                query=query,
            )
        else:
            if isinstance(url, str):
                url = make_url(url)

            url = deepcopy(url)
            if host is not None:
                self._set_url(url, "host", host)
            if database is not None:
                self._set_url(url, "database", database)
            if username is not None:
                self._set_url(url, "username", username)
            if password is not None:
                self._set_url(url, "password", password)
            if port is not None:
                self._set_url(url, "port", port)
            if query is not None:
                self._set_url(url, "query", query)

        self._url = url
        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
        self._host = self.engine.url.host
        self._port = self.engine.url.port
        self._database = self.engine.url.database
        self._username = self.engine.url.username
        self._password = self.engine.url.password
        self._query = self.engine.url.query

    @property
    def _params(self):
        return (self._host, self._port, self._database, self._username)

    @db_repet
    def execute_sql(self, sql: str, **kwargs) -> pd.DataFrame:
        """执行 SQL 语句"""
        return pd.read_sql(sql=sql, con=self.engine, **kwargs)

    @abstractmethod
    def get_table_columns(self, table: str, schema: str = None) -> list[str]:
        """获取表字段名"""
        pass

    @db_repet
    @global_cache
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: tuple[str] = None,
        symbols: tuple[str] = None,
        query: tuple[str] = None,
        schema: str = None,
        date_name: str = "date",
        index_col: tuple[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = None,
        is_cache: bool = False,
        **kwargs,
    ):
        """
        读取 SQL 数据
        """

        cols = self.get_table_columns(table=table, schema=schema)

        if index_col is not None:
            if isinstance(index_col, (int, str)):
                index_col = [index_col]
            index_col = [cols[c] if isinstance(c, int) else c for c in index_col]
            index_col = [c for c in index_col if c in cols]
        if fields is not None and index_col is not None:
            if isinstance(fields, str):
                fields = [fields]
            fields: pd.Index = pd.Index(fields)
            fields = fields.union(index_col, sort=False)
        if index_col is not None and len(index_col) == 0:
            index_col = None

        SQL = self._gen_sql(
            table=table,
            start=start,
            end=end,
            fields=fields,
            symbols=symbols,
            query=query,
            schema=schema,
            date_name=date_name,
            oper="SELECT",
            other_sql=other_sql,
            op_format=op_format,
        )

        df: pd.DataFrame = self.execute_sql(SQL, index_col=index_col, **kwargs)

        if df.empty:
            return df

        df = df.replace(["None"], np.nan)
        if index_col is not None:
            if is_sort_index:
                df.sort_index(inplace=True)
            if is_drop_duplicate_index:
                df = df.loc[~df.index.duplicated()]

        return df

    @db_repet
    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        if_exists: str = "append",
        index: bool = False,
        is_drop_duplicate_index: bool = False,
        **kwargs,
    ) -> bool:
        """
        保存 dataframe 到 SQL 数据库
        """
        if df.empty:
            return False

        if df.index.names[0] is not None:
            df = df.sort_index()
            if is_drop_duplicate_index:
                df = df.pipe(lambda x: x.loc[~x.index.duplicated()])

        if df.index.names[0] is not None:
            df = df.reset_index()

        res = df.to_sql(
            name=table,
            con=self.engine,
            if_exists=if_exists,
            index=index,
            schema=schema,
            **kwargs,
        )

        return bool(res)

    def remove(
        self,
        table: str,
        start: str = None,
        end: str = None,
        date_name: str = "date",
        query: list[str] = None,
        schema: str = None,
        **kwargs,
    ) -> bool:
        """
        删除数据
        """

        SQL = self._gen_sql(
            table=table,
            start=start,
            end=end,
            fields=None,
            symbols=None,
            query=query,
            schema=schema,
            date_name=date_name,
            oper="DELETE",
        )

        res = self.execute_sql(SQL, **kwargs)

        return bool(res)
