from typing import List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.url import URL

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE
from dbframe.utility import gen_sql


def _list2str(lst: List[str]):
    return str(tuple(lst)).replace(",)", ")")


class MysqlDB(DatabaseTemplate):
    """
    Mysql 数据库
    """
    def __init__(
        self,
        url: str = None,
        host: str = None,
        port: str = None,
        database: str = None,
        username: str = None,
        password: str = None,
    ) -> None:
        if url is None:
            url = URL(
                'mysql+pymysql',
                host=host,
                port=port,
                database=database,
                username=username,
                password=password,
            )
        self.url = url
        self.engine = create_engine(url)
        self.host = self.engine.url.host
        self.port = self.engine.url.port
        self.database = self.engine.url.port
        self.username = self.engine.url.username
        self.password = self.engine.url.password

        self._read_df_cache = lru_cache(CACHE_SIZE)(self._read_df)

    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: Tuple[str] = None,
        symbols: Tuple[str] = None,
        query: Tuple[str] = None,
        date_name: str = 'date',
        index_col: Tuple[str] = [0, 1],
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
            fields = fields.union(index_col)
        if index_col is not None and not len(index_col):
            index_col = None

        SQL = gen_sql(
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
        index_col: Tuple[str] = 0,
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
        **kwargs,
    ) -> bool:
        """
        保存 dataframe 到 mysql 数据库
        """
        if df.empty:
            return False

        if df.index.names[0] is not None:
            df = df.sort_index().pipe(lambda x: x.loc[~x.index.duplicated()])

        if mode == 'update' and df.index.names[
                0] is not None and table in self.engine.table_names():

            idx = df.index
            if isinstance(idx, pd.MultiIndex):
                query_del = [
                    f"{name} in {_list2str(idx.levels[i].astype(str))}"
                    for i, name in enumerate(idx.names)
                ]
            else:
                query_del = [f"{idx.name} in {_list2str(idx.astype(str))}"]

            _df_store: pd.DataFrame = self._read_df(engine=self.engine,
                                                    table=table,
                                                    query=query_del,
                                                    index_col=idx.names)
            if not _df_store.empty:
                _df_store = _df_store.sort_index().pipe(
                    lambda x: x.loc[~x.index.duplicated()])
                _idx = _df_store.reindex(_df_store.index.difference(idx)).index
                df = df.append(_df_store.reindex(_idx)).sort_index()

                sql_del = gen_sql(table, query=query_del, oper='DELETE')
                self.engine.execute(sql_del)

        if df.index.names[0] is not None:
            df = df.reset_index()

        df.to_sql(table,
                  self.engine,
                  if_exists=if_exists,
                  index=index,
                  **kwargs)

        return True

    @property
    def tables(self):
        return [x[0] for x in self.engine.execute("show tables").fetchall()]

    def __hash__(self) -> int:
        return hash(self.url)


def read_sql(
    database: MysqlDB,
    table: str,
    start: str = None,
    end: str = None,
    fields: Tuple[str] = None,
    symbols: Tuple[str] = None,
    query: Tuple[str] = None,
    date_name: str = 'date',
    index_col: Tuple[str] = 0,
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
        **kwargs,
    )
