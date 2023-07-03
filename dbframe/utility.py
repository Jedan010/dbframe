from functools import wraps
import logging
from time import sleep


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