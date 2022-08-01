from itertools import groupby
from operator import itemgetter
from typing import Any, Dict, List, Tuple, Union

import numpy as np
import pandas as pd
from lz4.block import compress as lz4_compress
from lz4.block import decompress as lz4_decompress
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from dbframe.cache import lru_cache
from dbframe.database_api import DatabaseTemplate
from dbframe.setting import CACHE_SIZE

lz4_compressHC = lambda _str: lz4_compress(_str, mode='high_compression')

INDEX_COLLECTION = 'index'
# START = 'start'
# END = 'end'
START = 'start_date'
END = 'end_date'
SEGMENT = 'segment'
DATA = 'data'
METADATA = 'metadata'


class MongoDataFrameDB(DatabaseTemplate):
    """
    易于存取 DataFrame 的 Mongodb 数据库 API
    对于大数据进行压缩处理
    """
    def __init__(
        self,
        database: str,
        host: str = 'localhost',  # '10.101.247.24',
        port: int = 27017,
        MAX_CHUNK_SIZE: int = 15 * 1024 * 1024,
    ) -> None:
        """
        Parameters
        ----------
        database : str
            数据库名称
        host : str, optional
            数据库地址, by default '10.101.247.24'
        port : int, optional
            数据库端口, by default 27017
        MAX_CHUNK_SIZE : int, optional
            每条数据最大长度
        """
        self._host = host
        self._port = port
        self._database = database
        self.client: MongoClient = MongoClient(host=host, port=port)
        self.database: Database = self.client[database]
        self.MAX_CHUNK_SIZE: int = MAX_CHUNK_SIZE

        self._read_df_cache = lru_cache(CACHE_SIZE)(self._read_df)

    def _read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: Dict[str, Any] = None,
        date_name: str = 'date',
        index_cols: Union[int, str, List[int], List[str]] = 0,
        is_sort_index: bool = True,
        is_drop_duplicates: bool = True,
        is_compress: bool = False,
        **kwargs,
    ):
        if is_compress:
            func = self.read_df_compress
        else:
            func = self.read_df_uncompress
        return func(
            table=table,
            start=start,
            end=end,
            fields=fields,
            symbols=symbols,
            query=query,
            date_name=date_name,
            index_cols=index_cols,
            is_sort_index=is_sort_index,
            is_drop_duplicates=is_drop_duplicates,
            **kwargs,
        )

    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: Dict[str, Any] = None,
        date_name: str = 'date',
        index_cols: Union[int, str, List[int], List[str]] = 0,
        is_sort_index: bool = True,
        is_drop_duplicates: bool = True,
        is_compress: bool = False,
        is_cache: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """
        读取 Mongo DataFrame 数据

        Parameters
        ----------
        table : str
            数据表名称
        start : str | datetime, optional
            起始日期, by default None
        end : str | datetime, optional
            结束日期, by default None
        fields : tuple | str, optional
            列名称列表, 如果为 str, 得到为 Series, by default None
        symbols : tuple | str, optional
            (如果有股票, 指数等) 选定的标的的名称列表
        query : dict, optional
            查询条件, by default None
        index_cols : tuple[str] | tuple[int] | str | int, optional
            选定作为 index 的列的名称或者序号, by default 0
        sort : tuple[tuple], optional
            排序的字段和方法序列, by default None
        limit : int, optional
            返回数量, by default None
        is_sort_index : bool, optional
            是否根据 index 排序, by default True
        date_name : str, optional
            日期字段名称, by default 'date'
        is_sort_index : bool, optional
            是否根据 index 排序, by default True
        is_drop_duplicates: bool, optiona;
            是否去重, by default True
        is_compress : bool
            数据是否压缩
        is_cache : bool
            数据是否缓存
            
        Returns
        -------
        DataFrame | Series
            查询的结果
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
            index_cols=index_cols,
            is_sort_index=is_sort_index,
            is_drop_duplicates=is_drop_duplicates,
            is_compress=is_compress,
            **kwargs,
        )

    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        is_compress: bool = False,
        mode: str = 'update',
        groupby_name: Union[str, List[str]] = 'date',
        chunk_period: str = 'D',
        date_name: str = 'date',
        **kwargs,
    ) -> bool:
        """
        保存数据至 Mongo 数据库
        """
        if not is_compress:
            return self.save_df_uncompress(
                df=df,
                table=table,
                mode=mode,
            )
        else:
            return self.save_df_compress(
                df=df,
                table=table,
                groupby_name=groupby_name,
                chunk_period=chunk_period,
                date_name=date_name,
                **kwargs,
            )

    @property
    def tables(self):
        return self.database.list_collection_names()

    def insert_df(self, df: pd.DataFrame, table: str) -> bool:
        """
        将 DataFrame 插入至 MongoDB

        Parameters
        ----------
        df : DataFrame
            要插入的数据
        table : str
            要插入数据库的表名称
        """
        if df.empty:
            return False
        if isinstance(df, pd.Series):
            df = df.to_frame()
        collection: Collection = self.database[table]
        if df.index.names[0] is not None:
            df = df.reset_index()
        docs = df.to_dict('records')
        collection.insert_many(docs)
        return True

    def update_df(self, df: pd.DataFrame, table: str) -> bool:
        """
        将 DataFrame 更新至 MongoDB,
        更新按照 df 的 index, df 必须先设置 index

        Parameters
        ----------
        df : DataFrame
            要插入的数据
        table : str
            要更新数据库的表名称

        Raises
        ------
        ValueError
            更新MongDB {table} 的dataframe 应该先设置index
        """

        if df.empty:
            return False
        if isinstance(df, pd.Series):
            df = df.to_frame()
        collection: Collection = self.database[table]
        if df.index.names[0] is None:
            raise ValueError(f'更新 MongDB {table} 的 dataframe 应该先设置 index!')

        keys = df.index.to_frame(index=False).to_dict('records')
        docs = df.to_dict('records')
        for key, doc in zip(keys, docs):
            collection.update_one(key, {"$set": doc}, upsert=True)
        return True

    def save_df_uncompress(
        self,
        df: pd.DataFrame,
        table: str,
        mode: str = 'update',
    ) -> bool:
        """
        将 dataframe 保存至 MongoDB
        如果 df 有 index 则默认为更新模式

        Parameters
        ----------
        df : DataFrame
            要插入或更新的数据
        table : str
            数据库的表名称
        mode : str, optional
            保存数据的模式 'update' 或者 'insert' 
            by default 'update'            
        """

        if mode == 'update' \
            and df.index.names[0] is not None \
            and table in self.database.list_collection_names() \
            and self.database[table].estimated_document_count() != 0:
            return self.update_df(df, table)
        return self.insert_df(df, table)

    def read_df_uncompress(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: Dict[str, Any] = None,
        index_cols: Union[int, str, List[int], List[str]] = 0,
        date_name: str = 'date',
        sort: List = None,
        limit: int = 0,
        is_sort_index: bool = True,
        is_drop_duplicates: bool = True,
        **kwargs,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        从 MongoDB 中读取数据并转化为 DataFrame 

        Parameters
        ----------
        table : str
            数据表名称
        start : str | datetime, optional
            起始日期, by default None
        end : str | datetime, optional
            结束日期, by default None
        fields : tuple | str, optional
            列名称列表, 如果为 str, 得到为 Series, by default None
        symbols : tuple | str, optional
            (如果有股票, 指数等) 选定的标的的名称列表
        index_cols : tuple[str] | tuple[int] | str | int, optional
            选定作为 index 的列的名称或者序号, by default 0
        query : dict, optional
            查询条件, by default None
        date_name : str, optional
            日期字段的名称, by default 'date'
        sort : tuple[tuple], optional
            排序的字段和方法序列, by default None
        limit : int, optional
            返回数量, by default 0
        is_sort_index : bool, optional
            是否根据 index 排序, by default True
        is_drop_duplicates: bool, optiona;
            是否去重, by default True

        Returns
        -------
        DataFrame | Series
            查询的结果
        """

        # 判断表名是否存在
        if table not in self.database.list_collection_names():
            return pd.DataFrame()
        collection = self.database[table]

        if query is None:
            query = {}
        projection = {'_id': 0}

        # 筛选日期
        if start is not None or end is not None:
            query[date_name] = {}
        if start is not None:
            query[date_name]['$gte'] = pd.to_datetime(start)
        if end is not None:
            query[date_name]['$lte'] = pd.to_datetime(end)

        # 筛选标的
        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            query['symbol'] = {"$in": symbols}

        # 设定 index_col
        cols = list(collection.find_one({}, projection).keys())
        if index_cols is not None:
            if isinstance(index_cols, int):
                index_cols = [cols[index_cols]]
            if isinstance(index_cols, str):
                index_cols = [index_cols]
            if isinstance(index_cols, list):
                index_cols = [
                    cols[c] if isinstance(c, int) else c for c in index_cols
                ]

        # 筛选列名称
        _col = None  # 用作记录 fields 是否为 str 以判断是否输出 Series
        if fields is not None:
            if isinstance(fields, str):
                _col = fields
                fields = [fields]
            if index_cols is not None:
                fields = set(fields) | set(index_cols)
            for c in fields:
                projection[c] = 1

        # 查询数据
        res = collection.find(query, projection, sort=sort, limit=limit)

        # 转化为DataFrame
        df = pd.DataFrame(res)
        if df.empty:
            return df
        if is_drop_duplicates:
            df.drop_duplicates(inplace=True)
        if index_cols is not None:
            df.set_index(index_cols, inplace=True)
            if is_sort_index:
                df.sort_index(inplace=True)
        if _col is not None:
            df = df[_col]
        return df

    def save_df_compress(
        self,
        df: pd.DataFrame,
        table: str,
        groupby_name: Union[str, Tuple[str]] = 'date',
        chunk_period: str = 'D',
        date_name: str = 'date',
        mode: str = 'update',
    ) -> bool:
        """
        将 dataframe 按特定指标压缩后(以更新的方式)存储至 MongoDB
        存储的格式为: `groupby_name`, start, end, segment, data, metedata

        Parameters
        ----------
        df : DataFrame
            要存入的数据
        table : str
            数据库的表名称
        groupby_name : tuple[str] | str, optional
            压缩指标名称, by default 'date'
        chunk_period : str, optional
            将数据压缩的期限, 必须 groupby_name 中包含 date_name 才生效
            例如: "D" 为按日压缩, "M" 为按月压缩, Y" 为按年压缩, by default 'D'
        date_name : str, optional
            日期字段名称, by default 'date'
        mode : str, optional
            保存数据模式, by default 'update'
        """

        if df.empty:
            return False

        if isinstance(df, pd.Series):
            df = df.to_frame()

        collection = self.database[table]

        # 压缩数据指标
        index_df = df.index.to_frame(index=False)
        if isinstance(groupby_name, str):
            if groupby_name == 'date':
                groupby_name = date_name
            groupby_name: list = [groupby_name]
        else:
            groupby_name = list(groupby_name)
        groupby_df: pd.DataFrame = index_df[groupby_name]
        if date_name in groupby_name and chunk_period is not None:
            groupby_df.loc[:, date_name] = pd.to_datetime(
                groupby_df[date_name]).dt.to_period(chunk_period)
        groupby_idx = pd.MultiIndex.from_frame(groupby_df)

        # 增加索引
        if INDEX_COLLECTION not in collection.index_information():
            index_collection: List[Tuple[str, int]] = [
                (x, ASCENDING) for x in groupby_name + [START, END, SEGMENT]
            ]
            self.add_index(table=table,
                           index=index_collection,
                           name=INDEX_COLLECTION,
                           unique=True)

        # 分批分块压缩存储数据
        for group in df.groupby(groupby_idx):
            group_val: str = group[0]
            _df: pd.DataFrame = group[1]
            _dates = _df.index.get_level_values(date_name)
            start = _dates.min()
            end = _dates.max()
            doc = self.serialize(_df)

            # 如果数据过大，则将数据分割个很多小块(chunk)分别存储
            for i in range(int(len(doc[DATA]) / self.MAX_CHUNK_SIZE + 1)):
                chunk = {
                    METADATA:
                    doc[METADATA],
                    DATA:
                    doc[DATA][i * self.MAX_CHUNK_SIZE:(i + 1) *
                              self.MAX_CHUNK_SIZE],
                }

                query = {START: start, END: end, SEGMENT: i}
                for k, v in zip(groupby_name, group_val):
                    query[k] = str(v)
                chunk.update(query)
                if mode == 'update':
                    collection.update_one(query, {"$set": chunk}, upsert=True)
                else:
                    collection.insert_one(chunk)

    def read_df_compress(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: Tuple[str] = None,
        symbols: Tuple[str] = None,
        query: Dict[str, Any] = None,
        date_name: str = 'date',
        sort: Tuple[Tuple[str, int]] = None,
        limit: int = 0,
        is_sort_index: bool = True,
        is_drop_duplicates: bool = True,
        **kwargs,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        从 MongoDB 中读取压缩数据, 将其解压缩后转化为 DataFrame 

        Parameters
        ----------
        table : str
            数据表名称
        start : str | datetime, optional
            起始日期, by default None
        end : str | datetime, optional
            结束日期, by default None
        fields : tuple | str, optional
            列名称列表, 如果为 str, 得到为 Series, by default None
        symbols : tuple | str, optional
            (如果有股票, 指数等) 选定的标的的名称列表
        query : dict, optional
            查询条件, by default None
        sort : tuple[tuple], optional
            排序的字段和方法序列, by default None
        limit : int, optional
            返回数量, by default None
        is_sort_index : bool, optional
            是否根据 index 排序, by default True
        date_name : str, optional
            日期字段名称, by default 'date'
            
        Returns
        -------
        DataFrame | Series
            查询的结果
        """

        # 判断表名是否存在
        if table not in self.database.list_collection_names():
            return pd.DataFrame([])
        collection = self.database[table]

        if query is None:
            query = {}
        projection = {'_id': 0}

        # 筛选日期
        if start is not None:
            query[END] = {"$gte": pd.to_datetime(start)}
        if end is not None:
            query[START] = {"$lte": pd.to_datetime(end)}

        # 读取数据
        segment_cursor = collection.find(query,
                                         projection,
                                         sort=sort,
                                         limit=limit)

        # 如果数据过大分块存储，则将数据按照 start_date 和 end_date 合并
        group_keys = set(collection.find_one({}, {"_id": 0}).keys())
        group_keys -= {DATA, METADATA, SEGMENT}
        grouper = itemgetter(*group_keys)

        res = []
        for _, segments in groupby(sorted(segment_cursor, key=grouper),
                                   key=grouper):
            segments = list(segments)
            chunks = {
                METADATA: segments[0][METADATA],
                DATA: b"".join([doc[DATA] for doc in segments]),
            }

            # 将数据解压缩
            res.append(self.deserialize(chunks, fields))

        if not len(res):
            return pd.DataFrame()

        # 合并每条数据
        df = pd.concat(res)
        if is_sort_index:
            df.sort_index(inplace=True)
        if is_drop_duplicates:
            df.drop_duplicates(inplace=True)

        if start is not None or end is not None:
            # 由于数据存储的频率可能不是日，所有进一步筛选日期
            _dates = df.index.get_level_values(date_name)
            _sta = pd.to_datetime(start) if start is not None else _dates.min()
            _end = pd.to_datetime(end) if end is not None else _dates.max()
            df = df.loc[(_dates >= _sta) & (_dates <= _end)]
        # df = df.loc[slice(start, end), ]

        if symbols is not None and 'symbol' in df.index.names:
            if isinstance(symbols, str):
                symbols = [symbols]
            df = df.loc[df.index.get_level_values('symbol').isin(symbols)]

        return df

    def serialize(self, df: pd.DataFrame) -> Dict:
        """
        序列化 DataFrame
        使用 LZ4 方式压缩数据
        """

        doc = {METADATA: {}, DATA: None}
        doc[METADATA]['index_names'] = df.index.names if df.index.names[
            0] is not None else None
        df: pd.DataFrame = df.reset_index()

        data: bytes = b""
        dtypes = {}
        lengths = {}
        columns = []
        n = 0
        for c in df:
            col = str(c)
            columns.append(str(c))
            arr = df[c].values

            if arr.dtype == 'object':
                arr = arr.astype(
                    f"U{pd._libs.writers.max_len_string_array(arr)}")
            dtypes[col] = arr.dtype.str

            # 压缩数据
            _data = lz4_compressHC(arr.tobytes())
            lengths[col] = (n, n + len(_data) - 1)
            data += _data
            n += len(_data)

        doc[DATA] = data
        doc[METADATA]['columns'] = columns
        doc[METADATA]['lengths'] = lengths
        doc[METADATA]['dtypes'] = dtypes

        return doc

    def deserialize(
        self,
        doc,
        cols: Union[str, Tuple[str]] = None,
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        反序列化 DataFrame
        将压缩的数据还原回 DataFrame

        Parameters
        ----------
        doc : dict
            序列化后的数据
        cols : list[str] | str, optional
            字段序列, by default None

        Returns
        -------
        DataFrame | Series
            反序列化后的数据
        """
        _col = None  # 用于针对str的col，以得到Series而不是DataFrame
        index_names = doc[METADATA]['index_names']
        if cols is None:
            cols = doc[METADATA]['columns']
        elif isinstance(cols, str):
            _col = cols
            cols = [cols]
        if index_names is not None:
            cols = list(set(cols) | set(index_names))

        # 解压缩数据
        df = pd.DataFrame({
            col: self._deserialize(doc, col)
            for col in cols if col in doc[METADATA]['columns']
        })

        if index_names:
            df = df.set_index(index_names)
        if _col is not None and _col in df:
            df = df[_col]
        return df

    def _deserialize(self, doc: dict, col: str) -> np.ndarray:
        """
        解压缩数据
        """
        lns = doc[METADATA]['lengths'][col]
        arr = doc[DATA][lns[0]:lns[1] + 1]
        dtyp = doc[METADATA]['dtypes'][col]
        return np.frombuffer(lz4_decompress(arr), dtype=dtyp)

    def get_last_data(self, table: str, is_compress: bool) -> pd.DataFrame:
        """
        获取最后一条数据

        Parameters
        ----------
        table : str
            数据表名称
        is_compress : bool
            数据是否压缩

        Returns
        -------
        DataFrame
            查询得到的数据
        """

        if table not in self.database.list_collection_names(
        ) or self.database[table].find().count() == 0:
            return pd.DataFrame()

        if not is_compress:
            return self.read_df_uncompress(table=table,
                                           sort=[('_id', -1)],
                                           limit=1)
        query = self.database[table].find(
            {},
            {
                '_id': 0,
                DATA: 0,
                METADATA: 0,
                SEGMENT: 0,
            },
            sort=[('_id', -1)],
            limit=1,
        )
        return self.read_df_compress(table=table, query=query)

    def delete_data(self, table: str, query: dict) -> int:
        """删除数据
        
        Parameters
        ----------
        table : str
            数据表名称
        query : dict
            删除条件        
        """
        return self.database[table].delete_many(query)

    def add_index(
        self,
        table: str,
        index: List[Tuple[str, int]],
        name: str = None,
        unique: bool = False,
    ) -> str:
        """
        增加索引, 以加快查询速度

        Parameters
        ----------
        table : str
            数据表名
        index : list[tuple(str, int)]
            索引列表, 例如 [('date', 1)] 表示按'date'升序索引
        name : str, optional
            索引名称, by default None
        unique : bool, optional
            是否按索引限制唯一, by default False
        """
        collection = self.database[table]
        return collection.create_index(index, name=name, unique=unique)

    def __hash__(self) -> int:
        return hash((self._host, self._port, self._database))


def read_mongo(
    database: MongoDataFrameDB,
    table: str,
    start: str = None,
    end: str = None,
    fields: List[str] = None,
    symbols: List[str] = None,
    query: Dict[str, Any] = None,
    date_name: str = 'date',
    index_cols: Union[int, str, List[int], List[str]] = 0,
    is_sort_index: bool = True,
    is_drop_duplicates: bool = True,
    is_compress: bool = False,
    is_cache: bool = True,
    **kwargs,
) -> Union[pd.DataFrame, pd.Series]:
    """
    读取 Mongo DataFrame 数据

    Parameters
    ----------
    database : MongoDataFrameDB
        数据库名称
    table : str
        数据表名称
    start : str | datetime, optional
        起始日期, by default None
    end : str | datetime, optional
        结束日期, by default None
    fields : tuple | str, optional
        列名称列表, 如果为 str, 得到为 Series, by default None
    symbols : tuple | str, optional
        (如果有股票, 指数等) 选定的标的的名称列表
    query : dict, optional
        查询条件, by default None
    index_cols : tuple[str] | tuple[int] | str | int, optional
        选定作为 index 的列的名称或者序号, by default 0
    sort : tuple[tuple], optional
        排序的字段和方法序列, by default None
    limit : int, optional
        返回数量, by default None
    is_sort_index : bool, optional
        是否根据 index 排序, by default True
    date_name : str, optional
        日期字段名称, by default 'date'
    is_sort_index : bool, optional
        是否根据 index 排序, by default True
    is_drop_duplicates: bool, optiona;
        是否去重, by default True
    is_compress : bool
        数据是否压缩
    is_cache : bool
        数据是否缓存
        
    Returns
    -------
    DataFrame | Series
        查询的结果
    """
    return database.read_df(
        table=table,
        start=start,
        end=end,
        fields=fields,
        symbols=symbols,
        query=query,
        date_name=date_name,
        index_cols=index_cols,
        is_sort_index=is_sort_index,
        is_drop_duplicates=is_drop_duplicates,
        is_compress=is_compress,
        is_cache=is_cache,
        **kwargs,
    )


def save_mongo(
    database: MongoDataFrameDB,
    df: pd.DataFrame,
    table: str,
    is_compress: bool = False,
    mode: str = 'update',
    groupby_name: Union[str, Tuple[str]] = 'date',
    chunk_period: str = 'D',
    date_name: str = 'date',
    **kwargs,
) -> bool:
    """
    保存数据至 Mongo 数据库
    """
    return database.save_df(
        df=df,
        table=table,
        is_compress=is_compress,
        mode=mode,
        groupby_name=groupby_name,
        chunk_period=chunk_period,
        date_name=date_name,
        **kwargs,
    )
