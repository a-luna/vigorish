"""decorators.retry"""
from functools import wraps
from time import sleep


class RetryLimitExceededError(Exception):
    """Implement retry logic on a decorated function."""

    def __init__(self, func, max_attempts):
        message = (
            f"Retry limit exceeded! (function: {func.__name__}, max attempts: {max_attempts})"
        )
        super().__init__(message)


def handle_failed_attempt(func, remaining, ex, delay):
    """Example function that could be supplied to on_failure attribute of retry decorator."""
    message = (
        f"Function name: {func.__name__}\n"
        f"Error: {repr(ex)}\n"
        f"{remaining} attempts remaining, retrying in {delay} seconds..."
    )
    print(message)


def retry(*, max_attempts=2, delay=1, exceptions=(Exception,), on_failure=None):
    """Retry the wrapped function when an exception is raised until max_attempts have failed."""

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = reversed(range(max_attempts))
            for remaining in attempts:
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
