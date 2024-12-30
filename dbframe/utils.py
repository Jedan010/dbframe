import logging
from functools import partial, update_wrapper, wraps
from time import sleep
from typing import Callable, Type


def repeat(
    func: Callable = None,
    n_repeat: int = 3,
    error_type: tuple[Type[Exception]] = (Exception,),
    logging_info: str = None,
    sleep_time: int = 1,
):
    """
    重复执行函数

    Parameters
    ----------
    n_repeat : int, optional
        重复次数, by default 3
    error_type : tuple of Exception types, optional
        报错类型, by default (Exception,)
    logging_info : str, optional
        报错信息, by default None
    sleep_time : int, optional
        重复间隔, by default 1
    """

    def decorator_repeat(func: Callable) -> Callable:
        @wraps(func)
        def wrapper_repeat(*args, **kwargs):
            for attempt in range(n_repeat):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not isinstance(e, error_type):
                        logging.error(f"Unexpected Error Occurred: {e}")
                        raise e
                    if attempt == n_repeat - 1:
                        logging.error(
                            f"{logging_info}, {e}. Attempt {attempt + 1} Failed."
                        )
                        raise e
                    logging.warning(
                        f"{logging_info}, {e}. "
                        f"Attempt {attempt + 1} Failed. "
                        f"Retrying..."
                    )
                    if attempt < n_repeat - 1:
                        sleep(sleep_time)

        return wrapper_repeat

    if func is not None and callable(func):
        return decorator_repeat(func)

    return decorator_repeat


# 动态导入数据库错误类型
def get_database_error_types():
    error_types = []
    try:
        from sqlalchemy.exc import SQLAlchemyError

        error_types.append(SQLAlchemyError)
    except ImportError:
        pass

    try:
        from clickhouse_driver.errors import ServerException

        error_types.append(ServerException)
    except ImportError:
        pass

    try:
        from pymysql.err import MySQLError

        error_types.append(MySQLError)
    except ImportError:
        pass

    try:
        from psycopg2 import OperationalError as PostgresOperationalError

        error_types.append(PostgresOperationalError)
    except ImportError:
        pass

    try:
        from sqlite3 import OperationalError as SQLiteOperationalError

        error_types.append(SQLiteOperationalError)
    except ImportError:
        pass

    try:
        from pyodbc import Error as SqlServerError

        error_types.append(SqlServerError)
    except ImportError:
        pass

    try:
        from pymongo.errors import PyMongoError

        error_types.append(PyMongoError)
    except ImportError:
        pass

    try:
        from hdfs import HdfsError

        error_types.append(HdfsError)
    except ImportError:
        pass

    try:
        from pyarrow.lib import ArrowIOError

        error_types.append(ArrowIOError)
    except ImportError:
        pass

    return tuple(error_types)


# 默认的数据库错误类型
DEFAULT_DB_ERROR_TYPES = get_database_error_types()


def db_repet(
    *args,
    error_type=DEFAULT_DB_ERROR_TYPES,
    logging_info="Database Error Occurred",
    **kwargs,
):
    return repeat(*args, error_type=error_type, logging_info=logging_info, **kwargs)


db_repet = wraps(db_repet)(repeat)
