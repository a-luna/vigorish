import time

from unittest import TestCase

from app.main.util.decorators import timeout, retry, RetryLimitExceededError


@timeout(seconds=1)
def sleep():
    time.sleep(2)


@retry(max_attempts=2, delay=1, exceptions=(TimeoutError,))
@timeout(seconds=1)
def retry_with_timeout():
    time.sleep(4)


class TestDecorators(TestCase):
    def test_timeout(self):
        with self.assertRaises(TimeoutError):
            sleep()

    def test_retry_with_timeout(self):
        with self.assertRaises(RetryLimitExceededError):
            retry_with_timeout()
