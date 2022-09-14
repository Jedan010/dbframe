import re
from typing import List

import numpy as np
import pandas as pd
from clickhouse_driver import Client
from sqlalchemy import create_engine

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE
from dbframe.utility import gen_sql


class ClickHouseDB(Client, DatabaseTemplate):
    """
    ClickHouse 数据库
    """

    cls_dict = {}

    def __init__(
        self,
        host: str = 'localhost',
        database: str = 'default',
        user: str = 'default',
        password: str = "",
        tcp_port: int = 9000,
        http_port: int = 8123,
        compression: bool = False,
        settings: dict = {'use_numpy': True},
        cache_size: int = CACHE_SIZE,
        *args,
        **kwargs,
    ):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._tcp_port = tcp_port
        self._http_port = http_port
        self._compression = compression
        self.engine = create_engine(
            f"clickhouse://{user}:{password}@{host}:{http_port}/{database}")

        kwargs['host'] = host
        kwargs['database'] = database
        kwargs['user'] = user
        kwargs['password'] = password
        kwargs['port'] = tcp_port
        kwargs['compression'] = compression
        kwargs['settings'] = settings

        super().__init__(*args, **kwargs)

        self._read_df_cache = lru_cache(cache_size)(self._read_df)

    @property
    def tables(self):
        return [x[0] for x in self.execute('show tables')]

    @property
    def databases(self):
        return [x[0] for x in self.execute("show databases")]

    def get_column_types(self, table: str) -> pd.Series:
        df = self.query_dataframe(f"desc {table}")
        if df.empty:
            return df
        df = df.set_index('name')['type']
        return df

    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: List[str] = None,
        date_name: str = 'date',
        index_col: List[str] = 'auto',
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = True,
        other_sql: str = None,
        op_format: str = 'TabSeparatedWithNamesAndTypes',
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 clickhouse 数据 
        """
        if table not in self.tables:
            return pd.DataFrame()

        col_types = self.get_column_types(table)
        cols: pd.Index = col_types.index

        if col_types.get(date_name) == 'Date':
            if start:
                start = pd.to_datetime(start).strftime("%Y-%m-%d")
            if end:
                end = pd.to_datetime(end).strftime("%Y-%m-%d")

        if index_col == 'auto':
            try:
                ddl: str = self.execute(f'show create {table}')[0][0]
                index_col = re.findall(r'ORDER BY [(]?([^())]*)[)]?\n',
                                       ddl)[0].split(',')
                index_col = [x.strip() for x in index_col]
            except Exception as e:
                pass
        elif index_col is not None:
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

        df: pd.DataFrame = self.query_dataframe(SQL, **kwargs)

        if df.empty:
            return df

        if index_col is not None and index_col != 'auto':
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
        fields: List[str] = None,
        symbols: List[str] = None,
        query: List[str] = None,
        date_name: str = 'date',
        index_col: List[str] = 'auto',
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = True,
        other_sql: str = None,
        op_format: str = 'TabSeparatedWithNamesAndTypes',
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
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

    def cread_table(
        self,
        df: pd.DataFrame,
        table: str,
        is_compress: bool = False,
        compress_type: str = 'LZ4HC',
        compress_level: int = 9,
        is_partition: bool = False,
        date_name=None,
    ):
        """创建一个表"""

        MAPPING = {
            'object': 'String',
            'uint64': 'UInt64',
            'uint32': 'UInt32',
            'uint16': 'UInt16',
            'uint8': 'UInt8',
            'float64': 'Float64',
            'float32': 'Float32',
            'int64': 'Int64',
            'int32': 'Int32',
            'int16': 'Int16',
            'int8': 'Int8',
            # 'bool': 'bool',
            'datetime64[D]': 'Date',
            'datetime64[ns]': 'DateTime'
        }

        if df.index.names[0] is not None:
            index_str = ", ".join(df.index.names)
            df = df.reset_index()
        else:
            index_str = str(df.columns[0])

        dtypes_df = df.dtypes.replace(MAPPING)
        if is_compress:
            if compress_type == 'LZ4':
                dtypes_df = dtypes_df + f"  CODEC({compress_type})"
            elif compress_type == 'LZ4HC':
                dtypes_df = dtypes_df + f"  CODEC({compress_type}({compress_level}))"

        dtypes_str = dtypes_df.to_string().replace("\n", ",\n")

        if date_name is None:
            _date_dtype = dtypes_df[dtypes_df.str.startswith('Date')]
            date_name = _date_dtype.index[0] if not _date_dtype.empty else None
        partition_str = f"PARTITION BY toYYYYMM({date_name})" \
                        if date_name is not None and is_partition else ""

        sql_create = f"""
            CREATE TABLE IF NOT EXISTS {table}
            (
                {dtypes_str}
            )
            ENGINE MergeTree()
            ORDER BY ({index_str})
            {partition_str}
            """

        self.execute(sql_create)

    def chg_df_dtype(self, df: pd.DataFrame, table: str):
        """转换 dataframe 数据类型与表内类型一致"""
        MAPPING_REVERSE = {
            'String': str,
            'UInt64': np.int64,
            'UInt32': np.int32,
            'UInt16': np.int16,
            'UInt8': np.int8,
            'Float64': np.float64,
            'Float32': np.float32,
            'Int64': np.int64,
            'Int32': np.int32,
            'Int16': np.int16,
            'Int8': np.int8,
            'Date': np.datetime64,
            'DateTime': np.datetime64,
        }
        table_type = self.get_column_types(table).replace(MAPPING_REVERSE)
        df = df.apply(lambda x: x.astype(table_type[x.name]))
        return df

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        is_partition: bool = False,
        date_name: str = None,
        is_compress: bool = False,
        compress_type: str = 'LZ4HC',
        compress_level: int = 9,
        is_drop_duplicate_index: bool = False,
    ) -> int:

        if df.empty:
            return 0

        if isinstance(df, pd.Series):
            df = df.to_frame()

        for col in df.select_dtypes([bool]):
            df[col] = df[col].astype('uint8')
        for col in df.dtypes.loc[lambda x: x.eq('object')].index:
            df[col] = df[col].replace({
                'None': np.nan
            }).astype(float, errors='ignore')

        if table not in self.tables:
            self.cread_table(
                df,
                table,
                is_compress=is_compress,
                compress_type=compress_type,
                compress_level=compress_level,
                is_partition=is_partition,
                date_name=date_name,
            )

        if df.index.names[0] is not None:
            if is_drop_duplicate_index:
                df = df.sort_index().pipe(
                    lambda x: x.loc[~x.index.duplicated()])
            df = df.reset_index()

        df = self.chg_df_dtype(df, table)

        return self.insert_dataframe(f"INSERT INTO {table} VALUES", df)

    def __hash__(self) -> int:
        return hash((self._host, self._tcp_port, self._database))

    @classmethod
    def from_url(cls, url):
        if url not in ClickHouseDB.cls_dict:
            ClickHouseDB.cls_dict[url] = super().from_url(url)
        return ClickHouseDB.cls_dict.get(url)


def read_ch(
    database: ClickHouseDB,
    table: str,
    start: str = None,
    end: str = None,
    fields: List[str] = None,
    symbols: List[str] = None,
    query: List[str] = None,
    date_name: str = 'date',
    index_col: List[str] = None,
    is_sort_index: bool = True,
    is_drop_duplicate_index: bool = True,
    other_sql: str = None,
    op_format: str = 'TabSeparatedWithNamesAndTypes',
    is_cache: bool = False,
    **kwargs,
):
    if isinstance(database, str):
        database = ClickHouseDB.from_url(database)
    elif not isinstance(database, ClickHouseDB):
        raise ValueError("client 只能是 CHDB 或者 地址字符串格式")

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


def save_ch(
    database: ClickHouseDB,
    df: pd.DataFrame,
    table: str,
    is_partition: bool = False,
    date_name: str = None,
    is_compress: bool = False,
    compress_type: str = 'LZ4HC',
    compress_level: int = 9,
) -> int:
    """
    保存 dataframe 数据至 clickhouse 数据库
    """
    if isinstance(database, str):
        database = ClickHouseDB.from_url(database)
    elif not isinstance(database, ClickHouseDB):
        raise ValueError("chdb 只能是 CHDB 或者 地址字符串格式")

    return database.save_df(
        df=df,
        table=table,
        is_compress=is_compress,
        compress_type=compress_type,
        compress_level=compress_level,
        is_partition=is_partition,
        date_name=date_name,
    )
