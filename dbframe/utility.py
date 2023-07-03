from functools import wraps
import logging
from time import sleep
from typing import List
import pandas as pd


def _list2str(lst: list[str]):
    if isinstance(lst, str):
        lst = [lst]
    return str(tuple(lst)).replace(",)", ")")


def gen_sql(
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
    if query is None:
        query = []
    elif isinstance(query, str):
        query = [query]
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
        symbols_str = _list2str(symbols)
        query.append(f"symbol in {symbols_str}")
    if fields is None:
        fields = '*'
    elif not isinstance(fields, str):
        fields = ', '.join(fields)
    where = "WHERE " + " AND ".join(query) if query else ""
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


def repeat(
    func: callable = None,
    num_times: int = 3,
    error_type=Exception,
    logging_info: str = None,
    sleep_time: int = 1,
):
    """
    重复执行函数

    Parameters
    ----------
    num_times : int, optional
        重复次数, by default 3
    error_type : Exception, optional
        报错类型, by default Exception
    logging_info : str, optional
        报错信息, by default None
    sleep_time : int, optional
        重复间隔, by default 1
    
    Returns
    -------
    function
        重复执行函数
    
    Examples
    --------
    >>> @repeat
    >>> def div(a, b):
    >>>     print(f"{a} / {b}")
    >>>     return a / b
    >>> div(1, 2)
    1 / 2
    0.5
    >>> div(1, 0)
    1 / 0
    1 / 0
    1 / 0
    Traceback (most recent call last):
    ...
    ZeroDivisionError: division by zero    
    """

    def decorator_repeat(func):

        @wraps(func)
        def wrapper_repeat(*args, **kwargs):
            nonlocal num_times
            flag = False
            res = None
            while not flag:
                try:
                    res = func(*args, **kwargs)
                    flag = True
                except error_type as e:
                    if num_times <= 1:
                        raise e
                    num_times -= 1
                    if logging_info:
                        logging.warning(logging_info)
                    sleep(sleep_time)
            return res

        return wrapper_repeat

    if callable(func):
        return decorator_repeat(func)
    else:
        return decorator_repeat