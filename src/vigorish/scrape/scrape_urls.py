import time
from random import randint

from tqdm import tqdm

from vigorish.enums import PythonScrapeTool
from vigorish.scrape.progress_bar import (
    url_batch_description,
    batch_timeout_description,
    url_fetch_description,
    save_html_description,
)
from vigorish.scrape.util import request_url, render_url
from vigorish.util.decorators.retry import RetryLimitExceededError
from vigorish.util.list_helpers import make_chunked_list, make_irregular_chunked_list
from vigorish.util.result import Result


def scrape_url_list(url_list, data_set, scrape_tool, config, scraped_data):
    scrape_params = config.get_all_url_scrape_params(data_set)
    if scrape_params["batchScrapingEnabled"]:
        batch_list = create_batch_list(url_list, scrape_params)
        total_urls = len(url_list)
        return scrape_urls_in_batches(
            batch_list, total_urls, data_set, scrape_tool, scrape_params, scraped_data
        )
    return scrape_urls(url_list, data_set, scrape_params, scraped_data)


def create_batch_list(url_list, scrape_params):
    uniform_batch_size = scrape_params.get("uniformBatchSize", 50)
    min_batch_size = scrape_params.get("minBatchSize", 0)
    max_batch_size = scrape_params.get("maxBatchSize", 0)
    if min_batch_size and max_batch_size:
        return make_irregular_chunked_list(url_list, min_batch_size, max_batch_size)
    return make_chunked_list(url_list, uniform_batch_size)


def scrape_urls_in_batches(
    batch_list, total_urls, data_set, scrape_tool, scrape_params, scraped_data
):
    with tqdm(total=len(batch_list), unit="batch", position=0, leave=False) as pbar_batch:
        with tqdm(total=total_urls, unit="url", position=1, leave=False) as pbar_url:
            with tqdm(total=1, unit="seconds", position=2, leave=False) as pbar_timeout:
                for batch_number, batch in enumerate(batch_list, start=1):
                    pbar_batch.set_description(url_batch_description(data_set, len(batch), True))
                    pbar_display = "Executing job batch..."
                    pbar_timeout.set_description(batch_timeout_description(data_set, pbar_display))
                    result = scrape_url_batch(
                        batch,
                        total_urls,
                        data_set,
                        scrape_tool,
                        scrape_params,
                        scraped_data,
                        pbar_url,
                    )
                    pbar_batch.update()
                    if batch_number < len(batch_list):
                        next_batch_size = len(batch_list[batch_number])
                        pbar_batch.set_description(
                            url_batch_description(data_set, next_batch_size, False)
                        )
                        if scrape_params["batchTimeoutRequired"]:
                            batch_timeout(data_set, scrape_params, pbar_timeout)


def batch_timeout(data_set, scrape_params, pbar):
    uniform_timeout = scrape_params.get("batchTimeoutUniformMs", 3000)
    min_timeout = scrape_params.get("batchTimeoutMinMs", 0)
    max_timeout = scrape_params.get("batchTimeoutMaxMs", 0)
    timeout_ms = uniform_timeout
    if min_timeout and max_timeout:
        timeout_ms = randint(min_timeout, max_timeout)
    min_remaining = int(timeout_ms / float(60000))
    sec_remaining = int((timeout_ms - min_remaining * 60000) / float(1000))
    total_seconds = int(timeout_ms / float(1000))
    total_seconds_remaining = [1 for sec in range(total_seconds)]
    pbar.reset(total=total_seconds)
    for one_sec in total_seconds_remaining:
        pbar_display = f"Until Next Batch: {min_remaining:02d}:{sec_remaining:02d}"
        pbar.set_description(batch_timeout_description(data_set, pbar_display))
        pbar.update()
        time.sleep(one_sec)
        if not sec_remaining:
            min_remaining -= 1
            sec_remaining = 59
        else:
            sec_remaining -= 1
    pbar_display = "Executing job batch..."
    pbar.set_description(batch_timeout_description(data_set, pbar_display))
    pbar.reset()
    return


def scrape_url_batch(
    url_batch, total_urls, data_set, scrape_tool, scrape_params, scraped_data, pbar
):
    for url_details in url_batch:
        pbar.set_description(url_fetch_description(data_set, url_details["identifier"]))
        try:
            # html = fetch_url(scrape_tool, url_details["url"])
            pbar.set_description(save_html_description(data_set, url_details["identifier"]))
            # result = scraped_data.save_html(data_set, url_details["identifier"], html)
            # if result.failure:
            #    return result
            # url_details["html_filepath"] = result.value
            pbar.update()
            if scrape_params["urlTimeoutRequired"]:
                url_timeout(data_set, scrape_params)
        except RetryLimitExceededError as e:
            return Result.Fail(repr(e))
        except Exception as e:
            return Result.Fail(f"Error: {repr(e)}")


def fetch_url(scrape_tool, url):
    if scrape_tool == PythonScrapeTool.REQUESTS:
        return request_url(url)
    if scrape_tool == PythonScrapeTool.SELENIUM:
        return render_url(self.driver, url)
    return Result.Fail(f"SCRAPE_TOOL setting either not set or set incorrectly.")


def url_timeout(data_set, scrape_params):
    uniform_timeout = scrape_params.get("urlTimeoutUniformMs", 3000)
    min_timeout = scrape_params.get("urlTimeoutMinMs", 0)
    max_timeout = scrape_params.get("urlTimeoutMaxMs", 0)
    if min_timeout and max_timeout:
        random_timeout = randint(min_timeout, max_timeout) / float(1000)
        time.sleep(random_timeout)
        return
    time.sleep(uniform_timeout / float(1000))
    return


def scrape_urls(url_list, scrape_params):
    pass
