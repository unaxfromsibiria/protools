import importlib
import os
import random
import sys
import traceback
import uuid
from uuid import UUID

import ujson

from .options import service_state

try:
    from time import monotonic as current_time
except ImportError:
    from time import time as current_time


def get_time_uuid() -> uuid.UUID:
    """Create UUID by current time and main service UUID.
    """
    microseconds = (current_time() * 1e6)
    intervals = int(microseconds * 10) + 0x01b21dd213814000
    low = intervals & 0xffffffff
    mid = (intervals >> 32) & 0xffff
    hi_version = (intervals >> 48) & 0x0fff
    clock_rand = random.getrandbits(14)
    seq_low = clock_rand & 0xff
    variant = 0x80 | ((clock_rand >> 8) & 0x3f)

    return uuid.UUID(
        fields=(
            low, mid, hi_version, variant, seq_low, service_state["id"].node
        ),
        version=1
    )


def strip_module_path(filepath: str) -> str:
    """In fact name of module is sufficient.
    """
    try:
        _, pkg_name, file_name = filepath.rsplit("/", 2)
        return f"{pkg_name}/{file_name}"
    except ValueError:
        return filepath


def stacktrace_info(exc_info: tuple, splitter: str = "\n") -> str:
    """Stack in one line with splitters.
    """

    if splitter is None or not isinstance(splitter, str):
        splitter = " "

    try:
        _, _, exc_tb = exc_info
    except ValueError:
        return " "
    else:
        extracted_list = traceback.extract_tb(exc_tb)
        return splitter.join((
            "{}:{}".format(strip_module_path(filename.split("..")[-1]), lineno)
            for filename, lineno, *_ in extracted_list
        ))


def expand_traceback(splitter: str = " ") -> str:
    """Stack in one line with splitters.
    """

    if splitter is None:
        splitter = " "

    info = stacktrace_info(sys.exc_info(), splitter=splitter)
    return f"{splitter} {info}"


def exception_wrapper(err: Exception, splitter: str = "\n") -> str:
    """Description wrapper for exception.
    """
    err_msg = err.__class__.__name__
    traceback = expand_traceback(splitter=splitter)
    return (
        f"{err_msg} {splitter}Message: {err} "
        f"{splitter}Traceback: {traceback}"
    )


def object_path(obj, to_str: bool = True):
    """For serialize some classes.
    """
    if to_str:
        return "{}.{}".format(obj.__module__, obj.__name__)
    else:
        module_path, obj_attr = obj.rsplit('.', 1)
        return getattr(
            importlib.import_module(module_path), obj_attr, None)


def path_to_obj(path, to_str: bool = True):
    """For serialize some classes.
    """
    return object_path(path, to_str=False)


def env_var_line(key: str) -> str:
    """Reading a environment variable as text.
    """
    return str(os.environ.get(key) or "").strip()


def env_var_int(key: str) -> int:
    """Reading a environment variable as int.
    """
    try:
        return int(env_var_line(key))
    except (ValueError, TypeError):
        return 0


def env_var_float(key: str) -> int:
    """Reading a environment variable as float.
    """
    try:
        return float(env_var_line(key))
    except (ValueError, TypeError):
        return 0


def env_var_bool(key: str) -> bool:
    """Reading a environment variable as binary.
    """
    return env_var_line(key).upper() in ("TRUE", "ON", "YES")


def env_var_list(key: str) -> list:
    """Reading a environment variable as list,
    source line should be divided by commas.
    """
    return list(
        filter(
            None, map(str.strip, env_var_line(key).split(","))
        )
    )


class CacheValueManager:
    """Common cache manager.
    """

    def params_key(self, event: UUID) -> str:
        """Create cache key.
        """
        return f"proserver:params:{event.hex}"

    def params_to_value(self, params: dict) -> str:
        """Return data as string to record in cache.
        """
        return ujson.dumps(params)

    def value_to_params(self, data: str) -> dict:
        """Return data as string to record in cache.
        """
        return ujson.loads(data)

    def result_key(self, event: UUID) -> str:
        """Result of methods new key.
        """
        r_part = uuid.uuid4().hex[:8]
        return f"proserver:result:{event.hex}:{r_part}"
