"""decorators.log_call"""
import inspect
import logging
from datetime import datetime
from functools import wraps
from time import sleep

from vigorish.util.datetime_util import format_timedelta_str
from vigorish.util.dt_format_strings import DT_NAIVE


class RetryLimitExceededError(Exception):
    """Custom error raised by retry decorator when max_attempts have failed."""

    def __init__(self, func, max_attempts):
        error = f"Retry limit exceeded! (function: {func.__name__}, max attempts: {max_attempts})"
        super().__init__(error)


def retry(
    *,
    max_attempts=2,
    delay=1,
    exceptions=(Exception,),
    on_failure=None,
):
    """Retry the wrapped function when an exception is raised until max_attempts have failed."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for remaining in reversed(range(max_attempts)):
                try:
                    return func(*args, **kwargs)
                except exceptions as ex:
                    if remaining > 0:
                        if on_failure:
                            on_failure(func, remaining, ex, delay)
                        sleep(delay)
                    else:
                        raise RetryLimitExceededError(func, max_attempts) from ex
                else:
                    break

        return wrapper

    return decorator


class LogCall:
    """Log call signature and execution time of decorated function."""

    def __init__(self, logger=None):
        self.logger = logger

    def __call__(self, func):
        if not self.logger:
            logging.basicConfig()
            self.logger = logging.getLogger(func.__module__)
            self.logger.setLevel(logging.INFO)

        @wraps(func)
        def wrapper(*args, **kwargs):
            func_call_args = get_function_call_args(func, *args, **kwargs)
            exec_start = datetime.now()
            result = func(*args, **kwargs)
            exec_finish = datetime.now()
            exec_time = format_timedelta_str(exec_finish - exec_start)
            exec_start_str = exec_start.strftime(DT_NAIVE)
            self.logger.info(f"{exec_start_str} | {func_call_args} | {exec_time}")
            return result

        def get_function_call_args(func, *args, **kwargs):
            """Return a string containing function name and list of all argument names/values."""
            func_args = inspect.signature(func).bind(*args, **kwargs)
            func_args.apply_defaults()
            func_args_str = ", ".join(f"{arg}={val}" for arg, val in func_args.arguments.items())
            return f"{func.__name__}({func_args_str})"

        return wrapper
