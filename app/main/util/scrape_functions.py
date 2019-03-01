"""Module contains functions that are needed by multiple scrapers."""
import os

import requests
from lxml import html
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec

from app.main.util.result import Result

def get_chromedriver(page_load_timeout=6000):
    """Initialize a Chrome webdriver instance with user-specified value for page load timeout."""
    max_attempts = 10
    attempts = 1
    while(attempts < max_attempts):
        try:
            options = webdriver.ChromeOptions()
            options.binary_location = os.getenv('GOOGLE_CHROME_BIN')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument("--test-type")
            options.add_argument('--pageLoadStrategy=none')
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--remote-debugging-port=9222')
            driver_path = os.getenv('CHROMEDRIVER_PATH')

            print('Initializing chromedriver/selenium...')
            driver = webdriver.Chrome(executable_path=driver_path, chrome_options=options)
            driver.set_page_load_timeout(page_load_timeout)
            print('Chromedriver was successfully initialized!')
            break
        except Exception as e:
            attempts += 1
            print('\nError: {r}'.format(r=repr(e)))
            if (attempts < max_attempts):
                continue
            else:
                error = repr(e)
                message = (
                    f'Unable to initialize chromedriver after {max_attempts} '
                    f'failed attempts, aborting task.\nError: {error}'
                )
                return Result.Fail(message)
    return Result.Ok(driver)


def request_url(url):
    """Send a HTTP request for URL, return the response if successful."""
    try:
        page = requests.get(url)
        response = html.fromstring(page.content, base_url=url)
        return Result.Ok(response)
    except Exception as e:
        error = 'Error: {error}'.format(error=repr(e))
        return Result.Fail(error)


def render_url(driver, url, max_attempts=100):
    """Fully render the URL (including JS), return the page content."""
    attempts = 1
    while(attempts < max_attempts):
        try:
            driver.get(url)
            page = driver.page_source
            response = html.fromstring(page, base_url=url)
            return Result.Ok(response)
        except Exception as e:
            attempts += 1
            exception_detail = 'Error: {r}'.format(r=repr(e))
            if (attempts < max_attempts):
                continue
            else:
                error = (
                    'Maximum number of attempts reached, unable to retrieve '
                    f'URL content\n{exception_detail}'
                )
                return Result.Fail(error)
