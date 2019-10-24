import time
from random import randint

from unittest import TestCase

from app.main.util.decorators import timeout, retry, measure_time, RetryLimitExceededError
from app.test.base import BaseTestCase


@timeout(seconds=1)
def sleep():
    time.sleep(2)


@retry(max_attempts=2, delay=1, exceptions=(TimeoutError,))
@timeout(seconds=1)
def retry_with_timeout():
    time.sleep(2)


@measure_time
def rand_time():
    time.sleep(1)
    time.sleep(randint(100, 150) / 100.0)


class TestDecorators(TestCase):
    def test_timeout(self):
        with self.assertRaises(TimeoutError):
            sleep()

    def test_retry_with_timeout(self):
        with self.assertRaises(RetryLimitExceededError):
            retry_with_timeout()

    def test_measure_time(self):
        rand_time()

#class TestUpdate(BaseTestCase):
    #def test_update(self):
