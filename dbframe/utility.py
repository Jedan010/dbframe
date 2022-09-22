from typing import List
import pandas as pd


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
        symbols_str = str(tuple(symbols)).replace(",)", ")")
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