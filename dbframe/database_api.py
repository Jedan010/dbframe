from abc import ABC, abstractmethod
from typing import List

import pandas as pd


class DatabaseTemplate(ABC):
    """数据库模板"""

    @abstractmethod
    def read_df(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fileds: List[str] = None,
        symbols: List[str] = None,
        query: List[str] = None,
        date_name: str = 'date',
        is_sort_index: bool = True,
        is_drop_duplicate_index: bool = False,
        is_cache: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        从数据库读取数据并转化为dataFrame格式
        """
        pass

    @abstractmethod
    def save_df(
        self,
        df: pd.DataFrame,
        table: str,
        **kwargs,
    ) -> bool:
        """
        保存 dataFrame 数据至数据库中
        """
        pass

    @property
    def tables(self) -> List[str]:
        """
        获取所有表
        """
        pass

    def __hash__(self) -> int:
        """
        hash 化数据库
        """
        pass

    def _list2str(self, lst: list[str]):
        if isinstance(lst, str):
            lst = [lst]
        return str(tuple(lst)).replace(",)", ")")

    def _format_query(self, query: List[str]) -> list[str]:
        if query is None:
            query = []
        elif isinstance(query, (str, dict, tuple)):
            query = [query]

        new_query: list[str] = []
        for x in query:
            if isinstance(x, str):
                new_query.append(x)
            elif isinstance(x, (list, tuple)):
                if len(x) == 2:
                    if x[1] is None:
                        continue
                    new_query.append(f"{x[0]} in {self._list2str(x[1])}")
                else:
                    raise ValueError(
                        f"query {x} must be a list or tuple with length 2")
            elif isinstance(x, dict):
                for k, v in x.items():
                    new_query.append(f"{k} in {self._list2str(v)}")
            else:
                raise ValueError(
                    f"query {x} must be a str or list or tuple or dict")

        return new_query

    def _gen_sql(
        self,
        table: str,
        start: str = None,
        end: str = None,
        fields: List[str] = None,
        symbols: List[str] = None,
        query: List[str] = None,
        date_name: str = 'date',
        oper: str = 'SELECT',
        other_sql: str = None,
        op_format: str = None,
    ) -> str:
        """
        生成 SQL 语句
        """
        query = self._format_query(query)
        if start:
            query.append(f"{date_name} >= '{start}'")
        if end:
            if date_name == 'datetime':
                _end = pd.to_datetime(end)
                if _end.hour == _end.minute == _end.second == 0:
                    end = _end.to_period("D").end_time.replace(microsecond=0,
                                                               nanosecond=0)
            query.append(f"{date_name} <= '{end}'")
        if symbols is not None:
            if isinstance(symbols, str):
                symbols = [symbols]
            symbols_str = self._list2str(symbols)
            query.append(f"symbol in {symbols_str}")

        query = [f"({x})" for x in query]
        where = "WHERE " + " AND ".join(query) if query else ""

        if fields is None:
            fields = '*'
        elif not isinstance(fields, str):
            fields = ', '.join(fields)
        if oper in ['delete', 'DELETE']:
            fields = ""

        other_sql = other_sql if other_sql else ""
        op_format = f"FORMAT {op_format}" if op_format else ""

        SQL = f"""
            {oper} {fields}
            FROM {table}
            {where}
            {other_sql}
            {op_format}
            """

        return SQL


def read_df(
    database: DatabaseTemplate,
    start: str = None,
    end: str = None,
    fileds: List[str] = None,
    symbols: List[str] = None,
    query: List[str] = None,
    date_name: str = 'date',
    is_sort_index: bool = True,
    is_drop_duplicate_index: bool = False,
    is_cache: bool = False,
    **kwargs,
) -> pd.DataFrame:
    return database.read_df(
        start=start,
        end=end,
        fileds=fileds,
        symbols=symbols,
        query=query,
        date_name=date_name,
        is_sort_index=is_sort_index,
        is_drop_duplicate_index=is_drop_duplicate_index,
        is_cache=is_cache,
        **kwargs,
    )


def save_df(database: DatabaseTemplate, df: pd.DataFrame, table: str,
            **kwargs) -> bool:
    return database.save_df(df=df, table=table, **kwargs)
