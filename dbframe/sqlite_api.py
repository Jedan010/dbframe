import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from dbframe.cache import global_cache
from dbframe.database_api import DatabaseTemplate


class SqliteDB(DatabaseTemplate):
    """
    Sqlite 数据库
    """

    def __init__(self, url: str = None, path: str = None) -> None:
        if url is None:
            url = f"sqlite:///{path}"
        self.url = url
        self.engine = create_engine(url)
        self.path = path

    def __str__(self):
        return f"SqliteDB(url={self.url})"

    @property
    def _params(self):
        return (self.url,)

    @property
    def tables(self):
        return pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'", self.engine
        )["name"].tolist()

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
        读取 sqlite 数据
        """

        if table not in self.tables:
            return pd.DataFrame()

        data_types: pd.Series = pd.read_sql_query(
            f"PRAGMA table_info({table})",
            self.engine,
            index_col="name",
        )["type"]
        cols = data_types.index

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

        df: pd.DataFrame = pd.read_sql_query(SQL, self.engine, **kwargs)

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

        df = df.replace(["None"], np.nan)
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

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        if_exists: str = "append",
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
        with self.engine.connect() as conn:
            res = conn.exec_driver_sql(f"DELETE FROM {table} WHERE {query}")
        return res
