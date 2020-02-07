"""Custom decorators."""
import inspect
from datetime import datetime
from functools import wraps
from signal import signal, alarm, SIGALRM
from time import sleep

from app.main.util.datetime_util import format_timedelta_str
from app.main.util.dt_format_strings import DT_NAIVE


def handle_failed_attempt(func, remaining, e, delay):
    """Example of the type of function that could be supplied to on_failure attribute."""
    message = (
        f"Function name: {func.__name__}\n"
        f"Error: {repr(e)}\n"
        f"{remaining} attempts remaining, retrying in {delay} seconds..."
    )
    print(message)


class RetryLimitExceededError(Exception):
    """Custom exception raised by retry decorator when max_attempts limit is reached."""

    def __init__(self, func, max_attempts):
        message = f"Retry limit exceeded! (function: {func.__name__}, max attempts: {max_attempts})"
        super().__init__(message)


def timeout(*, seconds=3, error_message="Call to function timed out"):
    """Abort the wrapped function if not finished after N seconds have elapsed."""

    def decorated(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        @wraps(func)
        def wrapper_timeout(*args, **kwargs):
            signal(SIGALRM, _handle_timeout)
            alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                alarm(0)
            return result

        return wraps(func)(wrapper_timeout)

    return decorated


def retry(*, max_attempts=2, delay=1, exceptions=(Exception,), on_failure=None):
    """Retry the wrapped function when an exception is raised until max_attempts have failed."""

    def decorated(func):
        @wraps(func)
        def wrapper_retry(*args, **kwargs):
            attempts = reversed(range(max_attempts))
            for remaining in attempts:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if remaining > 0:
                        if on_failure:
                            on_failure(func, remaining, e, delay)
                        sleep(delay)
                    else:
                        raise RetryLimitExceededError(func, max_attempts) from e
                else:
                    break

        return wrapper_retry

    return decorated


def measure_time(func):
    """Measure the execution time of the wrapped method."""

    @wraps(func)
    def wrapper_measure_time(*args, **kwargs):
        func_call_signature = get_call_signature(func, *args, **kwargs)
        exec_start = datetime.now()
        result = func(*args, **kwargs)
        exec_finish = datetime.now()
        exec_time = format_timedelta_str(exec_finish - exec_start)
        exec_start_str = exec_start.strftime(DT_NAIVE)
        print(f"{exec_start_str} | {func_call_signature} | {exec_time}")
        return result

    return wrapper_measure_time


def get_call_signature(func, *args, **kwargs):
    func_args = inspect.signature(func).bind(*args, **kwargs).arguments
    func_args_str = ", ".join(f"{arg}={val}" for arg, val in func_args.items())
    return f"{func.__name__}({func_args_str})"
