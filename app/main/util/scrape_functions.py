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
    RetryLimitExceededError
)
from app.main.util.result import Result


@retry(
    max_attempts=5, delay=5, exceptions=(TimeoutError, Exception))
@timeout(seconds=5)
def get_chromedriver(page_load_timeout=60):
    """Initialize a Chrome webdriver instance with user-specified value for page load timeout."""
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
    driver = webdriver.Chrome(executable_path=driver_path, options=options)
    driver.set_page_load_timeout(page_load_timeout)
    return driver


@retry(
    max_attempts=15, delay=5, exceptions=(TimeoutError, Exception))
@timeout(seconds=10)
def request_url(url):
    """Send a HTTP request for URL, return the response if successful."""
    page = requests.get(url)
    return html.fromstring(page.content, base_url=url)


@retry(
    max_attempts=15, delay=5, exceptions=(TimeoutError, Exception))
@timeout(seconds=10)
def render_url(driver, url):
    """Fully render the URL (including JS), return the page content."""
    driver.get(url)
    return html.fromstring(driver.page_source, base_url=url)
