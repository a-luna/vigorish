import time
import logging
from random import randint

import pytest

from vigorish.util.decorators import retry, LogCall, RetryLimitExceededError

logging.basicConfig()
log = logging.getLogger("custom_log")
log.setLevel(logging.INFO)
log.info("logging started")


def handle_failed_attempt(func, remaining, ex, delay):
    """Example function that could be supplied to on_failure attribute of retry decorator."""
    a_plural = "attempts" if remaining > 1 else "attempt"
    s_plural = "seconds" if delay > 1 else "second"
    message = (
        f"Function name: {func.__name__}\n"
        f"Error: {repr(ex)}\n"
        f"{remaining} {a_plural} remaining, retrying in {delay} {s_plural}..."
    )
    print(message)


@retry(max_attempts=3, delay=1, exceptions=(ValueError,), on_failure=handle_failed_attempt)
def try_parse_int(str_parse):
    int(str_parse)


def test_retry_decorator(capfd):
    with pytest.raises(RetryLimitExceededError):
        try_parse_int("invalid string")

    out, err = capfd.readouterr()
    assert "Function name: try_parse_int" in out
    assert "Error: ValueError(\"invalid literal for int() with base 10: 'invalid string'\")" in out
    assert "2 attempts remaining, retrying in 1 second..." in out
    assert "1 attempt remaining, retrying in 1 second..." in out
    assert not err


@LogCall()
def save_values(a, b, c):
    pass


@LogCall(log)
def rand_time(min=1, max=3, add_random=False):
    time.sleep(randint(min, max))
    if add_random:
        time.sleep(randint(100, 500) / 1000.0)


def test_default_logger(caplog):
    save_values("Aaron", "Charlie", "Ollie")
    logger, level, message = caplog.record_tuples[-1]
    assert logger == "tests.test_util_decorators"
    assert level == logging.INFO
    assert "save_values(a=Aaron, b=Charlie, c=Ollie)" in message


def test_custom_logger(caplog):
    rand_time(max=4, add_random=True)
    logger, level, message = caplog.record_tuples[-1]
    assert logger == "custom_log"
    assert level == logging.INFO
    assert "rand_time(min=1, max=4, add_random=True)" in message
