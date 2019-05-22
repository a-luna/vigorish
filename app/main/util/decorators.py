"""Custom decorators."""
import inspect
import sys
import time
from functools import wraps
from signal import signal, alarm, SIGALRM
from time import sleep


def handle_failed_attempt(func, remaining, e, delay):
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


def timeout(*, seconds=3, error_message="Function call timed out"):
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
    """Retry the wrapped function when an exception is raised and max_attempts limit not exceeded."""

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
        func_args = inspect.signature(func).bind(*args, **kwargs).arguments
        func_args_str =  ', '.join('{}={!r}'.format(*item) for item in func_args.items())
        ts = time.time()
        result = func(*args,**kwargs)
        te = time.time()
        print(f"{func.__name__} ({func_args_str}) {te-ts:0.2f} sec")
        return result
    return wrapper_measure_time
