"""Module contains functions that are needed by multiple scrapers."""
import os

import requests
from fake_useragent import UserAgent
from lxml import html
from selenium.webdriver import Chrome, ChromeOptions
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
    #ua = UserAgent()
    options = ChromeOptions()
    options.binary_location = os.getenv("GOOGLE_CHROME_BIN")
    #options.add_argument(f'--user-agent={ua.random}')
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    driver = Chrome(chrome_options=options, executable_path=driver_path)
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
