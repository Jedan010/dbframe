import pandas as pd
import numpy as np
from sqlalchemy import create_engine

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE
from dbframe.utility import gen_sql


class SqliteDB(DatabaseTemplate):
    """
    Sqlite 数据库
    """

    def __init__(self, url: str = None, path: str = None) -> None:
        if url is None:
            url = f"sqlite:///{path}"
        self.engine = create_engine(url)
        self.path = path

        self._read_df_cache = lru_cache(CACHE_SIZE)(self._read_df)

    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: tuple[str] = None,
        symbols: tuple[str] = None,
        query: tuple[str] = None,
        date_name: str = 'date',
        index_col: tuple[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = None,
        **kwargs,
    ):
        """
        读取 sqlite 数据 
        """

        if table not in self.tables:
            return pd.DataFrame()

        data_types: pd.Series = pd.read_sql_query(
            f'PRAGMA table_info({table})',
            self.engine,
            index_col='name',
        )['type']
        cols = data_types.index

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
            **kwargs,
        )

        if df.empty:
            return df

        MAPPING = {
            "INT": "int",
            "INTEGER": "int",
            "TINYINT": "int",
            "SMALLINT": "int",
            "MEDIUMINT": "int",
            "BIGINT": "int",
            "UNSIGNED BIG INT": "int",
            "INT2": "int",
            "INT8": "int",
            "CHARACTER(20)": "str",
            "VARCHAR(255)": "str",
            "VARYING CHARACTER(255)": "str",
            "NCHAR(55)": "str",
            "NATIVE CHARACTER(70)": "str",
            "NVARCHAR(100)": "str",
            "TEXT": "str",
            "CLOB": "str",
            "REAL": "float",
            "DOUBLE": "float",
            "DOUBLE PRECISION": "float",
            "FLOAT": "float",
            "DATETIME": "datetime64[ns]",
            "DATE": "datetime64[D]",
        }

        df = df.replace(['None'], np.nan)
        for col in df:
            if col not in data_types:
                continue
            if data_types[col] not in MAPPING:
                continue
            df[col] = df[col].astype(MAPPING[data_types[col]])

        if index_col is not None:
            df.set_index(index_col, inplace=True)
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
        fields: list[str] = None,
        symbols: list[str] = None,
        query: list[str] = None,
        date_name: str = 'date',
        index_col: list[str] = None,
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = None,
        is_cache: bool = False,
        **kwargs,
    ):
        """
        读取 sqlite 数据 
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
        保存 dataframe 到 sqlite 数据库
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

    def remove(self, table: str, query: str):
        """删除数据"""
        return self.engine.execute(f"DELETE FROM {table} WHERE {query}")

    @property
    def tables(self):
        return self.engine.table_names()

    def __hash__(self) -> int:
        return hash(self.url)
