import time
from random import randint

import click
from tqdm import tqdm

from app.cli.vig import cli
from app.main.config import get_config
from app.main.models.season import Season
from app.main.models.views.materialized_view import refresh_all_mat_views
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY, MONTH_NAME_SHORT
from app.main.util.result import Result


def scrape(ctx, data_set, start, end):
    """Scrape MLB data from websites."""
    engine = ctx.obj['engine']
    session = ctx.obj['session']
    result = __validate_date_range(session, start, end)
    if result.failure:
        return result
    date_range = get_date_range(start, end)

    result = get_config(data_set)
    if result.failure:
        return result
    scrape_config = result.value

    with tqdm(
        total=len(date_range),
        ncols=100,
        unit='day',
        mininterval=0.12,
        maxinterval=5,
        position=0
    ) as pbar:
        for scrape_date in date_range:
            pbar.set_description(f'Processing {scrape_date.strftime(DATE_ONLY)}....')
            result = __scrape_data_for_date(
                session,
                scrape_date,
                scrape_config
            )
            if result.failure:
                break

            session.commit()
            refresh_all_mat_views(engine, session)
            time.sleep(randint(250, 300)/100.0)
            pbar.update()

    session.close()
    if scrape_config.requires_selenium:
        scrape_config.driver.close()
        scrape_config.driver.quit()
    if result.failure:
        return result
    start_str = start.strftime(MONTH_NAME_SHORT)
    end_str = end.strftime(MONTH_NAME_SHORT)
    success = (
        'Requested data was successfully scraped:\n'
        f'data set....: {scrape_config.display_name}\n'
        f'date range..: {start_str} - {end_str}\n'
    )
    click.secho(success, fg='green')
    return Result.Ok()

def __validate_date_range(session, start, end):
    if start.year != end.year:
        error = (
            "Start and end dates must both be in the same year and within "
            "the scope of that year's MLB Regular Season."
        )
        return Result.Fail(error)
    if start > end:
        start_str = start.strftime(DATE_ONLY)
        end_str = end.strftime(DATE_ONLY)
        error = (
            '"start" must be a date before (or the same date as) "end":\n'
            f'start: {start_str}\n'
            f'end: {end_str}'
        )
        return Result.Fail(error)
    season = Season.find_by_year(session, start.year)
    start_date_valid = Season.is_date_in_season(session, start).success
    end_date_valid = Season.is_date_in_season(session, end).success
    if not start_date_valid or not end_date_valid:
        error = (
            f"Start and end date must both be within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}"
        )
        return Result.Fail(error)
    return Result.Ok()

def __scrape_data_for_date(session, scrape_date, scrape_config):
    input_dict = dict(date=scrape_date, session=session)
    if scrape_config.requires_input:
        result = scrape_config.get_input_function(scrape_date)
        if result.failure:
            return result
        input_dict['input_data'] = result.value

    if scrape_config.requires_selenium:
        input_dict['driver'] = scrape_config.driver
    result = scrape_config.scrape_function(input_dict)
    if result.failure:
        return result
    scraped_data = result.value

    if scrape_config.produces_list:
        result =  __upload_scraped_data_list(scraped_data, scrape_date, scrape_config)
    else:
        result = scrape_config.persist_function(scraped_data, scrape_date)
    if result.failure:
        return result

    return scrape_config.update_status_function(session, scraped_data)

def __upload_scraped_data_list(scraped_data, scrape_date, scrape_config):
    with tqdm(
        total=len(scraped_data),
        ncols=100,
        unit='file',
        mininterval=0.12,
        maxinterval=5,
        leave=False,
        position=1
    ) as pbar:
        for data in scraped_data:
            pbar.set_description(f'Uploading {data.upload_id}...')
            result = scrape_config.persist_function(data, scrape_date)
            if result.failure:
                return result
            time.sleep(randint(50, 100)/100.0)
            pbar.update()
    return Result.Ok()