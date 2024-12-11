import sys
from _thread import RLock
from collections import OrderedDict
from functools import wraps

import numpy as np
import pandas as pd

from dbframe.setting import LIMIT_SIZE, LIMIT_TYPE


def convert_to_tuple(obj):
    """将列表、Series、Index、ndarray转换为tuple"""
    if isinstance(obj, (list, tuple, pd.Index, pd.Series, np.ndarray)):
        return tuple(convert_to_tuple(x) for x in obj)
    return obj


class HashedSeq(list):
    __slots__ = "hashvalue"

    def __init__(self, tup, hash=hash):
        tup = convert_to_tuple(tup)
        self[:] = tup
        self.hashvalue = hash(tup)

    def __hash__(self):
        return self.hashvalue


class MemoryCache:
    """Memory Cache Unit."""

    def __init__(self, limit_size: int = 128, limit_type: str = "length"):
        self.limit_size = limit_size
        self.limit_type = limit_type
        self._size = 0
        self.od = OrderedDict()
        self.lock = RLock()

    def __setitem__(self, key, value):
        with self.lock:
            if key in self.od:
                self._size -= self._get_value_size(self.od[key])
            self._size += self._get_value_size(value)

            self.od.__setitem__(key, value)
            # move the key to end,make it latest
            self.od.move_to_end(key)

            if self.is_limited:
                # pop the oldest items beyond size limit
                while self._size > self.limit_size:
                    self.popitem(last=False)

    def __getitem__(self, key):
        with self.lock:
            v = self.od.__getitem__(key)
            self.od.move_to_end(key)
            return v

    def __contains__(self, key):
        return key in self.od

    def __len__(self):
        return self.od.__len__()

    def __repr__(self):
        return (
            f"{self.__class__.__name__}<max_size:{self.limit_size if self.is_limited else 'no limit'} "
            f"total_size:{self._size}>\n{self.od.__repr__()}"
        )

    def set_max_size(self, max_size: int):
        self.limit_size = max_size

    @property
    def is_limited(self):
        """whether memory cache is limited"""
        return self.limit_size > 0

    @property
    def total_size(self):
        return self._size

    def clear(self):
        with self.lock:
            self._size = 0
            self.od.clear()

    def get(self, key, default=None):
        return self.od.get(key, default)

    def popitem(self, last=True):
        with self.lock:
            k, v = self.od.popitem(last=last)
            self._size -= self._get_value_size(v)

            return k, v

    def pop(self, key):
        with self.lock:
            v = self.od.pop(key)
            self._size -= self._get_value_size(v)

            return v

    def _get_value_size(self, value):
        if self.limit_type == "length":
            return 1
        elif self.limit_type == "sizeof":
            return sys.getsizeof(value)
        raise ValueError("limit_type must be 'length' or 'sizeof'")


CACHE = MemoryCache(limit_size=LIMIT_SIZE, limit_type=LIMIT_TYPE)


def global_cache(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        is_cache = kwargs.pop("is_cache", False)

        _key = ("[database]", self.__class__.__name__)
        _key += self._params
        _key += ("[func]", func.__name__)
        _key += ("[args]", *args)
        _key += ("[kwargs]",)
        for item in kwargs.items():
            _key += item

        key = HashedSeq(_key)

        if key in CACHE:
            return CACHE[key]

        result = func(self, *args, **kwargs)

        if is_cache:
            CACHE[key] = result

        return result

    return wrapper
