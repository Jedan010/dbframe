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
