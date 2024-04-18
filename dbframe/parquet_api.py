import os
from copy import deepcopy
from functools import lru_cache
from typing import Callable

import pandas as pd
from pyarrow.lib import concat_tables
from pyarrow.parquet import read_table as read_parquet

from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE


class ParquetDB(DatabaseTemplate):
    """
    Parquet 数据库操作类
    """

    cls_dict = {}

    def __init__(
        self,
        path_data: str,
        cache_size: int = CACHE_SIZE,
    ):
        self.base_dir = path_data
        self.file_names = os.listdir(self.base_dir)
        self.date_names = [name.split(".")[0] for name in self.file_names]
        self._file_df = pd.Series(
            data=self.file_names, index=pd.to_datetime(self.date_names)
        ).sort_index()

        self._read_df_cache = lru_cache(maxsize=cache_size)(self._read_df)

    @property
    def tables(self):
        return self.date_names

    def _read_df(
        self,
        start: str,
        end: str,
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[tuple[str,str, str]] = None,
        date_filter:Callable = None,
        index_col: list[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        **kwargs,
    ):
        """
        读取 parquet 数据
        """
        files:pd.Series = self._file_df.loc[start:end]
        if files.empty:
            return pd.DataFrame()

        if query is None:
            query = []
        if isinstance(query, str):
            query = [query]

        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            query.append(("symbol", "in", symbols))

        if fields is not None:
            if isinstance(fields, str):
                fields = [fields]
            if index_col is not None:
                if isinstance(index_col, str):
                    index_col = [index_col]
                fields = pd.Index(fields).union(index_col, sort=False).tolist()

        tables = []
        for date, file_name in files.items():
            path = os.path.join(self.base_dir, file_name)
            filters = deepcopy(query)
            if date_filter is not None and callable(date_filter):
                _filter = date_filter(date)
                if isinstance(_filter, tuple):
                    filters.append(_filter)
                elif isinstance(_filter, list):
                    filters.extend(_filter)
            if len(filters) == 0:
                filters = None
            table = read_parquet(path, columns=fields, filters=filters, **kwargs)
            tables.append(table)    

        py_table = concat_tables(tables)
        df: pd.DataFrame = py_table.to_pandas()

        if index_col is not None:
            df = df.set_index(index_col)
            if is_sort_index:
                df = df.sort_index()
            if is_drop_duplicate_index:
                df = df.loc[~df.index.duplicated()]

        return df

    def read_df(
        self,
        start: str,
        end: str,
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[tuple[str, str, str]] = None,
        index_col: list[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        is_cache: bool = False,
        **kwargs,
    ):
        """
        读取 parquet 数据

        Parameters
        ----------
        start : str
            开始时间
        end : str
            结束时间
        fields : list[str], optional
            字段列表, 默认为 None
        symbols : list[str], optional
            股票列表, 默认为 None
        query : list[tuple[str, str, str]] | list[list[tuple[str, str, str]]], optional
            查询条件, 使用DNF范式, 默认为 None
            例如：[["symbol", "in", ["000001", "000002"]], ["date", ">", "2020-01-01"]]
            list[list[tuple[str, str, str]]] 为 OR 查询
            list[tuple[str, str, str]] 为 AND 查询
        index_col : list[str], optional
            索引列, 默认为 None
        is_sort_index : bool, optional
            是否按索引排序, 默认为 True
        is_drop_duplicate_index : bool, optional
            是否删除重复索引, 默认为 False
        is_cache : bool, optional
            是否使用缓存, 默认为 False

        Returns
        -------
        pd.DataFrame
            数据
        """

        if is_cache:
            func = self._read_df_cache
        else:
            func = self._read_df
        return func(
            start=start,
            end=end,
            fields=fields,
            symbols=symbols,
            query=query,
            index_col=index_col,
            is_sort_index=is_sort_index,
            is_drop_duplicate_index=is_drop_duplicate_index,
            **kwargs,
        )

    def save_df(self, df: pd.DataFrame, table: str, **kwargs) -> bool:
        pass
