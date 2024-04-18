from copy import deepcopy
from typing import Tuple

import numpy as np
import pandas as pd
import sqlalchemy
from pymysql.err import Error as PyMysqlError
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL, make_url

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE
from dbframe.utility import repeat


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
                host = 'localhost'
            if port is None:
                port = '3306'
            if database is None:
                database = 'default'
            if username is None:
                username = "root"
            if password is None:
                password = ""
            if query is None:
                query = {'charset': 'utf8'}
            url = URL(
                drivername='mysql+pymysql',
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
                if sqlalchemy.__version__ < '1.4':
                    url.host = host
                else:
                    url.set(host=host)
            if database is not None:
                if sqlalchemy.__version__ < '1.4':
                    url.database = database
                else:
                    url.set(database=database)
            if username is not None:
                if sqlalchemy.__version__ < '1.4':
                    url.username = username
                else:   
                    url.set(username=username)               
            if password is not None:
                if sqlalchemy.__version__ < '1.4':
                    url.password = password
                else:
                    url.set(password=password)
            if port is not None:
                if sqlalchemy.__version__ < '1.4':
                    url.port = port
                else:
                    url.set(port=port)
            if query is not None:
                if sqlalchemy.__version__ < '1.4':
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

        self._read_df_cache = lru_cache(CACHE_SIZE)(self._read_df)

    @repeat(error_type=(PyMysqlError, IOError))
    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: Tuple[str] = None,
        symbols: Tuple[str] = None,
        query: Tuple[str] = None,
        date_name: str = 'date',
        index_col: Tuple[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = None,
        **kwargs,
    ):
        """
        读取 mysql 数据 
        """
        cols: pd.Index = pd.read_sql_query(
            f'desc {table}',
            self.engine,
            index_col='Field',
        ).index

        if index_col is not None:
            if isinstance(index_col, (int, str)):
                index_col = [index_col]
            index_col = [
                cols[c] if isinstance(c, int) else c for c in index_col
            ]
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
            oper='SELECT',
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

        df = df.replace(['None'], np.nan)
        if index_col is not None:
            if is_sort_index:
                df.sort_index(inplace=True)

            if is_drop_duplicate_index:
                df = df.loc[~df.index.duplicated()]
        return df

    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: Tuple[str] = None,
        symbols: Tuple[str] = None,
        query: Tuple[str] = None,
        date_name: str = 'date',
        index_col: Tuple[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = None,
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 mysql 数据 
        """
        if is_cache:
            func = self._read_df_cache
        else:
            func = self._read_df
        return func(
            table=table,
            start=start,
            end=end,
            fields=fields,
            symbols=symbols,
            query=query,
            date_name=date_name,
            index_col=index_col,
            is_sort_index=is_sort_index,
            is_drop_duplicate_index=is_drop_duplicate_index,
            other_sql=other_sql,
            op_format=op_format,
            **kwargs,
        )

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        mode: str = 'insert',
        if_exists: str = 'append',
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

        # if mode == 'update' and df.index.names[
        #         0] is not None and table in self.engine.table_names():

        #     idx = df.index
        #     if isinstance(idx, pd.MultiIndex):
        #         query_del = [
        #             f"{name} in {_list2str(idx.levels[i].astype(str))}"
        #             for i, name in enumerate(idx.names)
        #         ]
        #     else:
        #         query_del = [f"{idx.name} in {_list2str(idx.astype(str))}"]

        #     _df_store: pd.DataFrame = self._read_df(engine=self.engine,
        #                                             table=table,
        #                                             query=query_del,
        #                                             index_col=idx.names)
        #     if not _df_store.empty:
        #         _df_store = _df_store.sort_index().pipe(
        #             lambda x: x.loc[~x.index.duplicated()])
        #         _idx = _df_store.reindex(_df_store.index.difference(idx)).index
        #         df = df.append(_df_store.reindex(_idx)).sort_index()

        #         sql_del = gen_sql(table, query=query_del, oper='DELETE')
        #         self.engine.execute(sql_del)

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
        date_name: str = 'date',
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
        query = ' AND '.join(query)

        return self.engine.execute(f"DELETE FROM {table} WHERE {query}")

    def drop_duplicate_date(
        self,
        table: str,
        index_col: list[str],
        start: str = None,
        end: str = None,
        date_name: str = 'date',
    ):
        """删除数据库中重复数据"""
        if start is None and end is None:
            df = self._read_df(table=table,
                               index_col=index_col,
                               is_drop_duplicate_index=True)
            self.engine.execute(f"DROP TABLE {table}")
            self.save_df(df=df, table=table)

        else:
            df = self._read_df(
                table=table,
                start=start,
                end=end,
                index_col=index_col,
                is_drop_duplicate_index=True,
            )
            self.remove(table=table, start=start, end=end, date_name=date_name)
            self.save_df(df=df, table=table)

    @property
    def tables(self):
        return self.engine.table_names()

    def __hash__(self) -> int:
        return hash(self._url)


def read_sql(
    database: MysqlDB,
    table: str,
    start: str = None,
    end: str = None,
    fields: Tuple[str] = None,
    symbols: Tuple[str] = None,
    query: Tuple[str] = None,
    date_name: str = 'date',
    index_col: Tuple[str] = None,
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
    return database.read_df(
        table=table,
        start=start,
        end=end,
        fields=fields,
        symbols=symbols,
        query=query,
        date_name=date_name,
        index_col=index_col,
        is_sort_index=is_sort_index,
        is_drop_duplicate_index=is_drop_duplicate_index,
        other_sql=other_sql,
        op_format=op_format,
        is_cache=is_cache,
        **kwargs,
    )


def save_sql(
    database: MysqlDB,
    df: pd.DataFrame,
    table: str,
    mode: str = 'insert',
    if_exists: str = 'append',
    index: bool = False,
    is_drop_duplicate_index: bool = False,
    **kwargs,
) -> bool:
    """
    保存 dataframe 到 mysql 数据库
    """
    return database.save_df(
        df=df,
        table=table,
        mode=mode,
        if_exists=if_exists,
        index=index,
        is_drop_duplicate_index=is_drop_duplicate_index,
        **kwargs,
    )
