import os

import pandas as pd
from fastparquet import write
from pyarrow.lib import TimestampType
from pyarrow.parquet import read_schema, read_table

from dbframe.cache import global_cache
from dbframe.database_api import DatabaseTemplate


class ParquetDB(DatabaseTemplate):
    """
    Parquet 数据库操作类
    """

    def __init__(self, path_data: str):
        self.base_dir = path_data

    def __str__(self):
        return f"ParquetDB(path={self.base_dir})"

    @property
    def _params(self):
        return (self.base_dir,)

    @property
    def tables(self):
        return [
            name[:-8] for name in os.listdir(self.base_dir) if name.endswith(".parquet")
        ]

    @global_cache
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[tuple[str, str, str]] = None,
        date_name: str = None,
        index_col: list[str] = "auto",
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 parquet 数据
        """

        if table not in self.tables:
            return pd.DataFrame()

        path_table = path = os.path.join(self.base_dir, table + ".parquet")
        metadata = read_schema(path_table)
        cols: list[str] = metadata.names

        if index_col == "auto":
            index_col: list[str] = metadata.pandas_metadata["index_columns"]
        elif index_col is not None:
            if index_col is None:
                if isinstance(index_col, (int, str)):
                    index_col = [index_col]
            index_col = [cols[c] if isinstance(c, int) else c for c in index_col]

        if index_col is not None and index_col != "auto":
            index_col_strict = [c for c in index_col if c in cols]

        if date_name is None:
            date_name = "date"
            if "date" not in cols and "datetime" in cols:
                date_name = "datetime"

        if "columns" in kwargs:
            _columns = kwargs.get("columns")
            del kwargs["columns"]
            if fields is None:
                fields = _columns

        if fields is not None:
            if isinstance(fields, str):
                fields = [fields]
            fields: pd.Index = pd.Index(fields)
            fields = fields.union(index_col_strict, sort=False).tolist()

        if query is None:
            query = []
        if isinstance(query, str):
            query = [query]

        if isinstance(
            metadata.types[metadata.get_field_index(date_name)], TimestampType
        ):
            if start is not None:
                query.append((date_name, ">=", pd.to_datetime(start)))
            if end is not None:
                query.append((date_name, "<=", pd.to_datetime(end)))

        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            query.append(("symbol", "in", symbols))

        if len(query) == 0:
            query = None

        parquet_table = read_table(path, columns=fields, filters=query, **kwargs)
        df: pd.DataFrame = parquet_table.to_pandas()

        if index_col:
            if df.index.names != index_col:
                df = df.reset_index().set_index(index_col)
            if is_sort_index:
                df = df.sort_index()
            if is_drop_duplicate_index:
                df = df.loc[~df.index.duplicated()]

        return df

    def save_df(self, df: pd.DataFrame, table: str, **kwargs) -> bool:
        """
        保存 datafraome 数据至 parquet 文件
        """

        if df.empty:
            return

        if isinstance(df, pd.Series):
            df = df.to_frame()

        path_table = os.path.join(self.base_dir, table + ".parquet")
        if table not in self.tables:
            return df.to_parquet(path_table, **kwargs)
        else:
            return write(filename=path_table, data=df, append=True)
