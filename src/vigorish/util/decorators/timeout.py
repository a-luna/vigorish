"""decorators.timeout"""
from functools import wraps
from signal import signal, alarm, SIGALRM


def timeout(*, seconds=3, error_message="Call to function timed out!"):
    """Abort the wrapped function call after the specified number of seconds have elapsed."""

    def _handle_timeout(signum, frame):
        raise TimeoutError(error_message)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            signal(SIGALRM, _handle_timeout)
            alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                alarm(0)
            return result

        return wrapper

    return decorator
