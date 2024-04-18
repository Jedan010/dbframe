from _thread import RLock
from functools import update_wrapper

import numpy as np
import pandas as pd


class _HashedSeq(list):
    __slots__ = "hashvalue"

    def __init__(self, tup, hash=hash):
        tup = (
            tuple(x) if isinstance(x, (list, pd.Index, pd.Series, np.ndarray)) else x
            for x in tup
        )
        self[:] = tup
        self.hashvalue = hash(tuple(tup))

    def __hash__(self):
        return self.hashvalue


def _make_key(args, kwds, kwd_mark=(object(),)):
    """Make a cache key from optionally typed positional and keyword arguments"""
    key = args
    if kwds:
        key += kwd_mark
        for item in kwds.items():
            key += item
    return _HashedSeq(key)


def _lru_cache_wrapper(user_function, maxsize):
    """lru cache"""
    sentinel = object()
    make_key = _make_key
    PREV, NEXT, KEY, VAL = 0, 1, 2, 3

    cache = {}
    full = False
    lock = RLock()
    root = []
    root[:] = [root, root, None, None]

    if maxsize <= 0:

        def wrapper(*args, **kwargs):
            # no cache
            return user_function(*args, **kwargs)

    elif maxsize is None:

        def wrapper(*args, **kwargs):
            # simple cache
            key = make_key(args, kwargs)
            res = cache.get(key, sentinel)
            if res is sentinel:
                res = user_function(*args, **kwargs)
                cache[key] = res
            return res

    else:

        def wrapper(*args, **kwargs):
            # size limited caching that tracks accesses by recency
            # 永远插在root的前一个位置
            # root的下一个作为最后一个
            nonlocal root, full
            key = make_key(args, kwargs)
            with lock:
                link = cache.get(key)
                if link is not None:
                    # Move the link to the front of the circular queue
                    # delete
                    link[NEXT][PREV] = link[PREV]
                    link[PREV][NEXT] = link[NEXT]
                    # insert to root prev
                    link[NEXT] = root
                    link[PREV] = root[PREV]
                    root[PREV][NEXT] = root[PREV] = link
                    return link[VAL]
            res = user_function(*args, **kwargs)
            with lock:
                if key in cache:
                    pass
                elif not full:
                    # insert to root prev
                    link = [root[PREV], root, key, res]
                    link[PREV][NEXT] = link[NEXT][PREV] = cache[key] = link
                    ## TODO 可以考虑换成cache的内存是否超过阈值
                    full = len(cache) >= maxsize
                else:
                    # link replace root
                    link = root
                    link[KEY] = key
                    link[VAL] = res
                    cache[key] = link

                    # root move to root next and del old data
                    root = root[NEXT]
                    del cache[root[KEY]]
                    root[KEY] = root[VAL] = None
            return res

        def cache_len():
            with lock:
                return len(cache)

        def cache_clear():
            nonlocal full
            with lock:
                cache.clear()
                root[:] = [root, root, None, None]
                full = False

        wrapper.cache_len = cache_len
        wrapper.cache_clear = cache_clear
        wrapper.__cache = cache
        return wrapper


def lru_cache(maxsize=10):
    """Least-recently-used cache decorator."""
    if callable(maxsize):
        user_function, maxsize = maxsize, 10
        wrapper = _lru_cache_wrapper(user_function, maxsize)
        return update_wrapper(wrapper, user_function)
    elif maxsize is None or isinstance(maxsize, int):

        def decorating_function(user_function):
            wrapper = _lru_cache_wrapper(user_function, maxsize)
            return update_wrapper(wrapper, user_function)

        return decorating_function
    else:
        raise TypeError("Expected first argument to be an integer, a callable, or None")
