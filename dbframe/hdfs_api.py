import os
from functools import wraps
from typing import Literal

import numpy as np
import pandas as pd

from dbframe.cache import global_cache
from dbframe.database_api import DatabaseTemplate


class HdfsDB(pd.HDFStore, DatabaseTemplate):
    """
    操作更安全的 HDFS 数据库
    """

    def __init__(
        self,
        path,
        mode: str = "r",
        complevel: int = None,
        complib: str = None,
        fletcher32: bool = False,
        **kwargs,
    ):
        if not os.path.isfile(path):
            mode = "a"
        super().__init__(
            path=path,
            mode=mode,
            complevel=complevel,
            complib=complib,
            fletcher32=fletcher32,
            **kwargs,
        )
        self.path = path
        self.mode = mode
        self.close()
        self._decorate_funcs()

    def __str__(self):
        return f"HdfsDB(path={self.path})"

    @property
    def _params(self):
        return (self.path,)

    @property
    def tables(self):
        return [x.replace("/", "") for x in self.keys()]

    def _decorate_funcs(self):
        names = [
            "select",
            "append",
            "put",
            "get",
            "remove",
            "__contains__",
            "__delitem__",
            "info",
            "keys",
            "save_df",
            "read_df",
        ]
        for name in names:
            func = getattr(self, name)
            func_decorated = self._safe_operation(func)
            setattr(self, name, func_decorated)

    def _safe_operation(self, func):
        @wraps(func)
        def decorated(*args, **kwargs):
            if func.__name__ in [
                "append",
                "save_df",
                "remove",
                "__delitem__",
                "put",
            ]:
                self.mode = "a"
            mode = self.mode
            try:
                self.open(mode=mode)
                res = func(*args, **kwargs)
            except Exception as e:
                raise e
            finally:
                self.close()
            return res

        return decorated

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        format: Literal["table", "fixed"] = "table",
        data_columns: list[str] = True,
        complib: Literal[
            "zlib", "lzo", "bzip2", "blosc", "blosc:lz4", "blosc:lz4hc"
        ] = None,
        complevel: int = None,
        chunksize: int = None,
        is_match_dtype: bool = True,
        is_drop_duplicate_index: bool = False,
        **kwargs,
    ) -> bool:
        """
        保存 DataFrame 数据
        """
        if df.empty:
            return False

        if format == "fixed":
            self.put(
                key=table, value=df, format=format, data_columns=data_columns, **kwargs
            )
            return True

        if is_match_dtype and self.__contains__(table):
            try:
                _df = self.select(table, start=-1)
                for c, d in _df.dtypes.iteritems():
                    if c in df:
                        df[c] = df[c].astype(d)
            except Exception:
                pass

        df = df.sort_index()
        if is_drop_duplicate_index:
            df = df.pipe(lambda x: x.loc[~x.index.duplicated()])

        if chunksize is None:
            self.append(
                key=table,
                value=df,
                format=format,
                data_columns=data_columns,
                complib=complib,
                complevel=complevel,
                **kwargs,
            )
        else:
            n = int(np.ceil(len(df) / chunksize))
            for i in range(n):
                _ista = int(i * chunksize)
                _iend = int((i + 1) * chunksize)
                _df = df.iloc[_ista:_iend]
                self.append(
                    key=table,
                    value=_df,
                    format=format,
                    complib=complib,
                    complevel=complevel,
                    **kwargs,
                )

        return True

    @global_cache
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[str] = None,
        date_name: str = "date",
        start_idx: int = None,
        stop_idx: int = None,
        index_cols: list[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 DataFrame 数据
        """

        if not self.__contains__(table):
            return pd.DataFrame()

        if (
            start is None
            and end is None
            and fields is None
            and symbols is None
            and query is None
        ):
            return self.select(
                key=table,
                start=start_idx,
                stop=stop_idx,
                **kwargs,
            )

        where = query
        if query is None:
            where = []
        elif isinstance(where, str):
            where = [where]
        _df: pd.DataFrame = self.select(table, start=-1)
        if _df.empty:
            return _df
        if start is not None or end is not None:
            if not isinstance(_df.index, pd.MultiIndex) and _df.index.name == date_name:
                date_name = "index"
            if start is not None:
                where.append(f'{date_name} >= "{start}"')
            if end is not None:
                where.append(f'{date_name} <= "{end}"')
        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols = list(symbols)
            symbol_name = "symbol"
            if (
                not isinstance(_df.index, pd.MultiIndex)
                and _df.index.name == symbol_name
            ):
                symbol_name = "index"
            where.append(f"{symbol_name} = {symbols}")
        if fields is not None and isinstance(fields, str):
            fields = [fields]

        df: pd.DataFrame = self.select(
            key=table,
            where=where,
            columns=fields,
            start=start_idx,
            stop=stop_idx,
            **kwargs,
        )

        if is_sort_index:
            df = df.sort_index()
        if is_drop_duplicate_index:
            df = df.loc[~df.index.duplicated()]

        return df
