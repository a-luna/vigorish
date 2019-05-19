"""Module contains functions that are needed by multiple scrapers."""
import os

import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from app.main.util.decorators import (
    timeout,
    retry,
    RetryLimitExceededError,
    handle_failed_attempt,
)
from app.main.util.result import Result


def get_chromedriver(page_load_timeout=6000):
    """Initialize a Chrome webdriver instance with user-specified value for page load timeout."""
    try:
        driver = _get_chromedriver(page_load_timeout)
        return Result.Ok(driver)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@retry(
    max_attempts=5,
    delay=5,
    exceptions=(TimeoutError, Exception),
    on_failure=handle_failed_attempt,
)
@timeout(seconds=5)
def _get_chromedriver(page_load_timeout):
    options = webdriver.ChromeOptions()
    options.binary_location = os.getenv("GOOGLE_CHROME_BIN")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--test-type")
    options.add_argument("--pageLoadStrategy=none")
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--remote-debugging-port=9222")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
    driver.set_page_load_timeout(page_load_timeout)
    return driver


def request_url(url):
    """Send a HTTP request for URL, return the response if successful."""
    try:
        page = requests.get(url)
        response = html.fromstring(page.content, base_url=url)
        return Result.Ok(response)
    except Exception as e:
        error = "Error: {error}".format(error=repr(e))
        return Result.Fail(error)


def render_url(driver, url, max_attempts=100):
    """Fully render the URL (including JS), return the page content."""
    try:
        response = _render_url(url)
        return Result.Ok(response)
    except RetryLimitExceededError as e:
        return Result.Fail(repr(e))
    except Exception as e:
        return Result.Fail(f"Error: {repr(e)}")


@retry(
    max_attempts=5,
    delay=5,
    exceptions=(TimeoutError, Exception),
    on_failure=handle_failed_attempt,
)
@timeout(seconds=30)
def _render_url(url):
    driver.get(url)
    page = driver.page_source
    return html.fromstring(page, base_url=url)
