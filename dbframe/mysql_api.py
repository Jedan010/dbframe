from copy import deepcopy

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url

from dbframe.cache import global_cache
from dbframe.database_api import DatabaseTemplate


class MysqlDB(DatabaseTemplate):
    """
    Mysql 数据库
    """

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
        if url is None:
            if host is None:
                host = "localhost"
            if port is None:
                port = "3306"
            if database is None:
                database = "default"
            if username is None:
                username = "root"
            if password is None:
                password = ""
            if query is None:
                query = {"charset": "utf8"}
            url = URL(
                drivername="mysql+pymysql",
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
                if sqlalchemy.__version__ < "1.4":
                    url.host = host
                else:
                    url.set(host=host)
            if database is not None:
                if sqlalchemy.__version__ < "1.4":
                    url.database = database
                else:
                    url.set(database=database)
            if username is not None:
                if sqlalchemy.__version__ < "1.4":
                    url.username = username
                else:
                    url.set(username=username)
            if password is not None:
                if sqlalchemy.__version__ < "1.4":
                    url.password = password
                else:
                    url.set(password=password)
            if port is not None:
                if sqlalchemy.__version__ < "1.4":
                    url.port = port
                else:
                    url.set(port=port)
            if query is not None:
                if sqlalchemy.__version__ < "1.4":
                    url.query = query
                else:
                    url.set(query=query)

        self._url = url
        self.engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
        self._host = self.engine.url.host
        self._port = self.engine.url.port
        self._database = self.engine.url.database
        self._username = self.engine.url.username
        self._password = self.engine.url.password
        self._query = self.engine.url.query

    def __str__(self):
        return f"MysqlDB(host={self._host}, database={self._database})"

    @property
    def _params(self):
        return (self._host, self._port, self._database, self._username)

    @property
    def tables(self):
        return self.engine.table_names()

    @global_cache
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: tuple[str] = None,
        symbols: tuple[str] = None,
        query: tuple[str] = None,
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
        读取 mysql 数据
        """
        cols: pd.Index = pd.read_sql_query(
            f"desc {table}",
            self.engine,
            index_col="Field",
        ).index

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
        if index_col is not None and not len(index_col):
            index_col = None

        SQL = self._gen_sql(
            table=table,
            start=start,
            end=end,
            fields=fields,
            symbols=symbols,
            query=query,
            date_name=date_name,
            oper="SELECT",
            other_sql=other_sql,
            op_format=op_format,
        )

        df: pd.DataFrame = pd.read_sql_query(
            SQL,
            self.engine,
            index_col=index_col,
            **kwargs,
        )

        if df.empty:
            return df

        df = df.replace(["None"], np.nan)
        if index_col is not None:
            if is_sort_index:
                df.sort_index(inplace=True)

            if is_drop_duplicate_index:
                df = df.loc[~df.index.duplicated()]
        return df

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        mode: str = "insert",
        if_exists: str = "append",
        index: bool = False,
        is_drop_duplicate_index: bool = False,
        **kwargs,
    ) -> bool:
        """
        保存 dataframe 到 mysql 数据库
        """
        if df.empty:
            return False

        if df.index.names[0] is not None:
            df = df.sort_index()
            if is_drop_duplicate_index:
                df = df.pipe(lambda x: x.loc[~x.index.duplicated()])

        if df.index.names[0] is not None:
            df = df.reset_index()

        df.to_sql(
            table,
            self.engine,
            if_exists=if_exists,
            index=index,
            **kwargs,
        )

        return True

    def remove(
        self,
        table: str,
        start: str = None,
        end: str = None,
        date_name: str = "date",
        query: list[str] = None,
    ):
        """删除数据"""
        if query is None and start is None and end is None:
            raise ValueError("query or start and end must be given")

        query = self._format_query(query=query)
        if start is not None:
            query.append(f"{date_name} >= '{start}'")
        if end is not None:
            query.append(f"{date_name} <= '{end}'")
        query = " AND ".join(query)

        with self.engine.connect() as conn:
            res = conn.exec_driver_sql(f"DELETE FROM {table} WHERE {query}")
        return res

    def drop_duplicate_date(
        self,
        table: str,
        index_col: list[str],
        start: str = None,
        end: str = None,
        date_name: str = "date",
    ):
        """删除数据库中重复数据"""
        if start is None and end is None:
            df = self.read_df(
                table=table, index_col=index_col, is_drop_duplicate_index=True
            )
            with self.engine.connect() as conn:
                conn.exec_driver_sql(f"DELETE FROM {table}")
            self.save_df(df=df, table=table)

        else:
            df = self.read_df(
                table=table,
                start=start,
                end=end,
                index_col=index_col,
                is_drop_duplicate_index=True,
            )
            self.remove(table=table, start=start, end=end, date_name=date_name)
            self.save_df(df=df, table=table)

    def get_table_date_desc(
        self,
        tables: list[str],
        start_date: str = None,
        end_date: str = None,
        index_col: list[str] = None,
        groupby_name: list[str] = None,
        date_name: str = "date",
        query: list[str] = None,
        including_table_name: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """
        获取表中数据的日期统计数据
        包括起始日期和结束日期和日期数量
        """

        if isinstance(tables, str):
            tables = [tables]

        res = []
        other_sql = None
        if groupby_name is not None:
            if isinstance(groupby_name, str):
                groupby_name = [groupby_name]
            if index_col is None:
                index_col = groupby_name
            other_sql = f"GROUP BY {','.join(groupby_name)}"

        for table in tables:
            _df = self.read_df(
                table=table,
                start=start_date,
                end=end_date,
                fields=[
                    f"MIN({date_name}) AS start_date",
                    f"MAX({date_name}) AS end_date",
                    f"COUNT(DISTINCT({date_name})) AS date_num",
                ],
                date_name=date_name,
                index_col=index_col,
                other_sql=other_sql,
                query=query,
                **kwargs,
            )

            if not _df.empty and including_table_name:
                _df = _df.assign(table_name=table)

            res.append(_df)

        if not res:
            return pd.DataFrame()

        res_df = pd.concat(res)

        return res_df
