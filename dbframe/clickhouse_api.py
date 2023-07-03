import re
from logging import warning
from typing import List

import numpy as np
import pandas as pd
from clickhouse_driver import Client
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE
from dbframe.utility import gen_sql


class ClickHouseDB(Client, DatabaseTemplate):
    """
    ClickHouse 数据库操作类 
    """

    cls_dict = {}

    def __init__(
        self,
        url: URL = None,
        host: str = None,
        database: str = None,
        user: str = None,
        password: str = None,
        http_port: int = None,
        tcp_port: int = 9000,
        compression: bool = False,
        settings: dict = {'use_numpy': True},
        cache_size: int = CACHE_SIZE,
        *args,
        **kwargs,
    ):
        """
        Parameters
        ----------
        url : URL, optional
            数据库 URL 地址, by default None
        host : str, optional
            数据库地址, by default None
        database : str, optional
            数据库名称, by default None
        user : str, optional
            用户名, by default None
        password : str, optional
            密码, by default None
        http_port : int, optional
            http 端口, by default None
        tcp_port : int, optional
            tcp 端口, by default 9000
        compression : bool, optional
            是否压缩, by default False
        settings : dict, optional
            设置, by default {'use_numpy': True}
        cache_size : int, optional
            缓存大小, by default CACHE_SIZE
        
        Examples
        --------
        >>> from dbframe import ClickHouseDB
        >>> chdb = ClickHouseDB(
        >>>     host='localhost',
        >>>     database='default',
        >>>     user='default',
        >>>     password='',
        >>>     http_port=8123,
        >>>     tcp_port=9000,
        >>>     compression=False,
        >>>     settings={'use_numpy': True},
        >>> )
        >>> chdb.read_df('table_name')
        >>> chdb.save_df(df, 'table_name')
        """

        if isinstance(url, str):
            url = URL(url)

        if not host:
            if url:
                host = url.host
            else:
                host = 'localhost'
        if not database:
            if url:
                database = url.database
            else:
                database = 'default'
        if not user:
            if url:
                user = url.username
            else:
                user = "default"
        if not password:
            if url:
                password = url.password
            else:
                password = ""
        if not http_port:
            if url:
                http_port = url.port
            else:
                http_port = 8123
        drivername: str = 'clickhouse'
        url = URL(
            drivername=drivername,
            host=host,
            database=database,
            username=user,
            password=password,
            port=http_port,
        )
        self._url = url
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._http_port = http_port
        self._tcp_port = tcp_port
        self._compression = compression
        try:
            self.engine = create_engine(self._url)
        except Exception:
            warning("没有安装 sqlalchemy-clickhouse 库")

        kwargs['host'] = self._host
        kwargs['database'] = self._database
        kwargs['user'] = self._user
        kwargs['password'] = self._password
        kwargs['port'] = self._tcp_port
        kwargs['compression'] = self._compression
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
        """获取表的列类型"""
        if table not in self.tables:
            return pd.Series()
        df = self.query_dataframe(f"desc {table}")
        if df.empty:
            return df
        df = df.set_index('name')['type']
        return df

    def get_column_names(
        self,
        table: str,
        exclude_names: list[str] = None,
    ) -> list[str]:
        """获取表的列名"""
        if table not in self.tables:
            return []
        df = self.get_column_types(table)
        names = df.index
        if df.empty:
            return names.to_list()
        if exclude_names is not None:
            if isinstance(exclude_names, str):
                exclude_names = [exclude_names]
            names = names.difference(exclude_names)
        return names.to_list()

    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: List[str] = None,
        date_name: str = None,
        index_col: List[str] = 'auto',
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
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

        if date_name is None:
            date_name = 'date'
            if 'date' not in col_types and 'datetime' in col_types:
                date_name = 'datetime'

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
            except Exception:
                index_col = None
                pass
        elif index_col is not None:
            if isinstance(index_col, (int, str)):
                index_col = [index_col]
            index_col = [
                cols[c] if isinstance(c, int) else c for c in index_col
            ]
            index_col = [c for c in index_col if c in cols]

        if 'columns' in kwargs:
            _columns = kwargs.get('columns')
            del kwargs['columns']
            if fields is None:
                fields = _columns

        if fields is not None and index_col is not None and index_col != 'auto':
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

        MAPPING = {
            'String': 'object',
            'UInt64': 'uint64',
            'UInt32': 'uint32',
            'UInt16': 'uint16',
            'UInt8': 'uint8',
            'Float64': 'float64',
            'Float32': 'float32',
            'Int64': 'int64',
            'Int32': 'int32',
            'Int16': 'int16',
            'Int8': 'int8',
            'Date': 'datetime64[D]',
            'DateTime': 'datetime64[ns]',
            'Nullable(String)': 'object',
            'Nullable(UInt64)': 'uint64',
            'Nullable(UInt32)': 'uint32',
            'Nullable(UInt16)': 'uint16',
            'Nullable(UInt8)': 'uint8',
            'Nullable(Float64)': 'float64',
            'Nullable(Float32)': 'float32',
            'Nullable(Int64)': 'int64',
            'Nullable(Int32)': 'int32',
            'Nullable(Int16)': 'int16',
            'Nullable(Int8)': 'int8',
            'Nullable(Date)': 'datetime64[D]',
            'Nullable(DateTime)': 'datetime64[ns]',
        }

        df = df.replace(['None'], np.nan)
        data_types = self.get_column_types(table)
        for col in df:
            if col not in data_types:
                continue
            if data_types[col] not in MAPPING:
                continue
            df[col] = df[col].astype(MAPPING[data_types[col]])

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
        date_name: str = None,
        index_col: List[str] = 'auto',
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        other_sql: str = None,
        op_format: str = 'TabSeparatedWithNamesAndTypes',
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 clickhouse 数据库数据, 转化为 dataframe 的格式

        Parameters
        ----------
        table : str
            表名
        start : str, optional
            开始日期, by default None
        end : str, optional
            结束日期, by default None
        fields : List[str], optional
            字段名, by default None
        symbols : List[str], optional
            代码列表, by default None
        query : List[str], optional
            查询语句, by default None
        date_name : str, optional
            日期字段名, by default None
        index_col : List[str], optional
            索引字段名, by default 'auto'
        is_sort_index : bool, optional
            是否按索引排序, by default True
        is_drop_duplicate_index : bool, optional
            是否删除重复索引, by default False
        other_sql : str, optional
            其他 sql 语句, by default None
        op_format : str, optional
            输出格式, by default 'TabSeparatedWithNamesAndTypes'
        is_cache : bool, optional
            是否缓存, by default False
        
        Returns
        -------
        pd.DataFrame
            dataframe 格式数据
        
        Examples
        --------
        >>> from dbframe import ClickHouseDB
        >>> chdb = ClickHouseDB(
        >>>     host='localhost',
        >>>     database='default',
        >>>     user='default',
        >>>     password='',
        >>>     http_port=8123,
        >>>     tcp_port=9000,
        >>>     compression=False,
        >>>     settings={'use_numpy': True},
        >>> )
        >>> chdb.read_df('table_name')
      
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
        df = df.apply(lambda x: x.astype(table_type[x.name], errors='ignore'))
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
        """
        保存 dataframe 数据至 clickhouse 数据库

        Parameters
        ----------
        df : pd.DataFrame
            dataframe 数据
        table : str
            表名
        is_partition : bool, optional
            是否分区, by default False
        date_name : str, optional
            日期字段名, by default None
        is_compress : bool, optional
            是否压缩, by default False
        compress_type : str, optional
            压缩类型, by default 'LZ4HC'
        compress_level : int, optional
            压缩等级, by default 9
        is_drop_duplicate_index : bool, optional
            是否删除重复索引, by default False
        
        Returns
        -------
        int
            插入的数据条数
        
        Examples
        --------
        >>> from dbframe import ClickHouseDB
        >>> chdb = ClickHouseDB(
        >>>     host='localhost',
        >>>     database='default',
        >>>     user='default',
        >>>     password='',
        >>>     http_port=8123,
        >>>     tcp_port=9000,
        >>>     compression=False,
        >>>     settings={'use_numpy': True},
        >>> )
        >>> chdb.save_df(df, 'table_name')

        Notes
        -----
        1. 如果表不存在, 则会自动创建表
        2. 如果表存在, 则会自动转换 dataframe 数据类型与表内类型一致       
        """

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

    def remove(
        self,
        table: str,
        start: str = None,
        end: str = None,
        date_name: str = None,
        query: list[str] = None,
    ):
        """删除数据"""
        if query is None and start is None and end is None:
            raise ValueError("query or start and end must be specified")

        col_types = self.get_column_types(table)
        if date_name is None:
            date_name = 'date'
            if 'date' not in col_types and 'datetime' in col_types:
                date_name = 'datetime'

        if query is None:
            query = []
        if isinstance(query, str):
            query = [query]
        if start is not None:
            query.append(f"{date_name} >= '{start}'")
        if end is not None:
            query.append(f"{date_name} <= '{end}'")
        query = ' AND '.join(query)

        return self.execute(f"""
            ALTER TABLE {table}
            DELETE WHERE {query}
        """)

    def get_table_ddl(self, table: str):
        ddl = self.execute(f"show create {table}")[0][0]
        return ddl

    def __hash__(self) -> int:
        return hash((self._host, self._tcp_port, self._database, self._user,
                     self._password))

    def get_latest_data(
        self,
        table: str,
        order_by: list[str] = 'auto',
        limit_num: int = 1,
        **kwargs,
    ):
        """
        获取最近更新的数据

        Parameters
        ----------
        table : str
            表名
        order_by : list[str], optional
            排序字段, by default 'auto'
        limit_num : int, optional
            获取数据条数, by default 1

        Returns
        -------
        pd.DataFrame
            dataframe 格式数据
        """
        if order_by == 'auto':
            ddl: str = self.execute(f'show create {table}')[0][0]
            order_by_str = re.findall(r'ORDER BY [(]?([^())]*)[)]?\n', ddl)[0]
        elif isinstance(order_by, str):
            order_by_str = order_by
        else:
            order_by_str = ", ".join(order_by)
        other_sql = f"ORDER BY ({order_by_str}) DESC LIMIT {limit_num}"
        return self.read_df(table=table, other_sql=other_sql, **kwargs)

    def get_last_date(
        self,
        table: str,
        order_by: list[str] = 'auto',
        limit_num: int = 1,
        date_name: str = None,
        fields: list[str] = [],
        **kwargs,
    ) -> pd.Timestamp:
        """
        取最近更新数据的日期

        Parameters
        ----------
        table : str
            表名
        order_by : list[str], optional
            排序字段, by default 'auto'
        limit_num : int, optional
            获取数据条数, by default 1
        date_name : str, optional
            日期字段名, by default None
        fields : list[str], optional
            字段名, by default []    

        Returns
        -------
        pd.Timestamp
            最近更新数据的日期
        """

        col_types = self.get_column_types(table)
        if date_name is None:
            date_name = 'date'
            if 'date' not in col_types and 'datetime' in col_types:
                date_name = 'datetime'

        return self.get_latest_data(
            table=table,
            order_by=order_by,
            limit_num=limit_num,
            date_name=date_name,
            fields=fields,
            index_col=[date_name],
            **kwargs,
        ).index[0]

    def get_all_last_date(
        self,
        tables: list[str] = None,
        filter_date: str = None,
    ):
        """
        获取所有表的最后更新日期

        Parameters
        ----------
        tables : list[str], optional
            表名列表, by default None
        filter_date : str, optional
            过滤日期, by default None
        
        Returns
        -------
        pd.Series
            最后更新日期
        """
        if tables is None:
            tables = [t for t in self.tables if not t.startswith('_')]
        if isinstance(tables, str):
            tables = [tables]
        res = {}
        for table in tables:
            try:
                res[table] = self.get_last_date(table)
            except Exception:
                pass
        last_date_df = pd.Series(res).reindex(tables)
        if filter_date is not None:
            last_date_df = last_date_df.loc[lambda x: x.lt(filter_date)]
        return last_date_df

    def read_df_multi(
        self,
        table_fields: dict[str, list[str]],
        start_date: str = None,
        end_date: str = None,
        is_drop_duplicate_index: bool = True,
        **kwargs,
    ):
        """
        从数据库中的多个表读取数据

        Parameters
        ----------
        table_fields : dict[str, list[str]]
            表名与字段名的字典
        start_date : str, optional
            开始日期, by default None
        end_date : str, optional
            结束日期, by default None
        
        Returns
        -------
        pd.DataFrame
            dataframe 格式数据
        
        Examples
        --------
        >>> from dbframe import ClickHouseDB
        >>> chdb = ClickHouseDB(
        >>>     host='localhost',
        >>>     database='default',
        >>>     user='default',
        >>>     password='',
        >>>     http_port=8123,
        >>>     tcp_port=9000,
        >>>     compression=False,
        >>>     settings={'use_numpy': True},
        >>> )
        >>> df = chdb.read_df_multi({'table1': ['field1', 'field2'], 
        >>>             'table2': ['field1', 'field2']})
        >>> df2 = chdb.read_df_multi({'table1': None,
        >>>             'table2': ['field1', 'field2']})

        Notes
        -----
        1. 读取多个表的数据, 并将数据以outer方式合并为一个 dataframe        
        """

        df = pd.DataFrame()
        if isinstance(table_fields, list):
            table_fields = {table: None for table in table_fields}
        for _table, fields in table_fields.items():
            _df = self.read_df(
                table=_table,
                start=start_date,
                end=end_date,
                fields=fields,
                is_drop_duplicate_index=is_drop_duplicate_index,
                **kwargs,
            )
            if _df.empty:
                continue
            if df.empty:
                df = _df
            else:
                df = df.join(_df, how='outer')
        if is_drop_duplicate_index:
            df = df.loc[~df.index.duplicated()]
        return df

    def drop_duplicate_data(
        self,
        table: str,
        start: str = None,
        end: str = None,
        date_name: str = None,
    ):
        """删除数据库中重复数据"""
        if start is None and end is None:
            df = self.read_df(table=table, is_drop_duplicate_index=True)
            self.execute(f'drop table if exists {table}')
            self.save_df(df, table)
        else:
            df = self.read_df(
                table=table,
                start=start,
                end=end,
                date_name=date_name,
                is_drop_duplicate_index=True,
            )
            self.remove(table=table, start=start, end=end, date_name=date_name)
            self.save_df(df, table)

    @classmethod
    def from_url(cls, url):
        """从 url 创建 ClickHouseDB 对象"""
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
    date_name: str = None,
    index_col: List[str] = None,
    is_sort_index: bool = True,
    is_drop_duplicate_index: bool = False,
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
