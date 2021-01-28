"""Retrieve a response from URL, will retry up to 5 times if the request does not succeed."""
from requests import get as requests_get
from requests.exceptions import ConnectionError, HTTPError, RequestException, Timeout

from vigorish.util.decorators import retry, RetryLimitExceededError
from vigorish.util.result import Result


def request_url_with_retries(url):
    @retry(max_attempts=5, delay=5, exceptions=(ConnectionError, HTTPError, Timeout, RequestException))
    def request_url(url):
        response = requests_get(url)
        response.raise_for_status()
        return response

    try:
        response = request_url(url)
        return Result.Ok(response)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
