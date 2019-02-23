"""CLI application entry point."""
import os
import time
from dateutil import parser
from pathlib import Path
from random import randint

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from config import scrape_config_by_data_set
from app.main.constants import MLB_DATA_SETS
from app.main.data.setup.initialize_database import initialize_database
from app.main.models.base import Base
from app.main.models.player import Player
from app.main.models.player_id import PlayerId
from app.main.models.runners_on_base import RunnersOnBase
from app.main.models.season import Season
from app.main.models.team import Team
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY, MONTH_NAME_SHORT
from app.main.util.scrape_functions import get_chromedriver

#TODO New tables: SeasonScrapeStatus, DayScrapeStatus, GameScrapeStatus, PitchAppScrapeStatus, PlayerScrapeStatus
#TODO New setup processes: create entries in season and day scrapestatus tables.
#TODO New cli command: vig status [YEAR], reports scrape status of mlb reg season for year provided

APP_ROOT = Path.cwd()
DOTENV_PATH = APP_ROOT / '.env'

class DateString(click.ParamType):
    name = 'date-string'
    def convert(self, value, param, ctx):
        try:
            date = parser.parse(value)
            return date
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can use '
                'any format recognized by dateutil.parser. For example, all '
                'of the strings below represent the same date and are valid\n'
                '"2018-5-13" -or- "05/13/2018" -or- "May 13 2018"'
            )
            self.fail(error, param, ctx)


@click.group()
@click.pass_context
def cli(ctx):
    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)

    engine = create_engine(os.getenv('DATABASE_URL'))
    session_maker = sessionmaker(bind=engine)
    ctx.obj = {
        'engine': engine,
        'sessionmaker': session_maker,
        'chrome_binary_path': os.getenv('GOOGLE_CHROME_BIN'),
        'chromedriver_path': os.getenv('CHROMEDRIVER_PATH')
    }


@cli.command()
@click.confirmation_option(
    prompt='Are you sure you want to delete all existing data?')
@click.pass_context
def setup(ctx):
    """Populate database with initial Player, Team and MLB Season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    session = ctx.obj['sessionmaker']()
    Base.metadata.drop_all(ctx.obj['engine'])
    Base.metadata.create_all(ctx.obj['engine'])
    result = initialize_database(session)
    if not result['success']:
        click.secho(result['message'], fg='red')
        session.close()
        return 1
    click.secho('Successfully populated database with initial data.\n', fg='green')
    session.close()
    return 0


@cli.command()
@click.option(
    '--data-set',
    type=click.Choice(MLB_DATA_SETS),
    prompt=True,
    show_choices=True,
    help='Data set to scrape from website.')
@click.option(
    '--start',
    type=DateString(),
    prompt=True,
    help=(
        'Date to start scraping data, string can be in any format that is '
        'recognized by dateutil.parser.'))
@click.option(
    '--end',
    type=DateString(),
    prompt=True,
    help=(
        'Date to stop scraping data, string can be in any format that is '
        'recognized by dateutil.parser.'))
@click.pass_context
def scrape(ctx, data_set, start, end):
    """Scrape MLB data sets from websites."""
    session = ctx.obj['sessionmaker']()
    result = __validate_date_range(session, start, end)
    if not result['success']:
        click.secho(result['message'], fg='red')
        session.close()
        return 1

    scrape_config = scrape_config_by_data_set[data_set]
    result = __get_driver(scrape_config)
    if not result['success']:
        click.secho(result['message'], fg='red')
        session.close()
        return 1
    driver = result['driver']

    date_range = get_date_range(start, end)
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
                scrape_config,
                driver
            )
            if not result['success']:
                break
            delay_ms = (randint(150, 250)/100.0)
            time.sleep(delay_ms)
            pbar.update()

    session.close()
    if scrape_config.REQUIRES_SELENIUM:
        driver.close()
        driver.quit()
    if not result['success']:
        click.secho(result['message'], fg='red')
        return 1
    start_str = start.strftime(MONTH_NAME_SHORT)
    end_str = end.strftime(MONTH_NAME_SHORT)
    success = (
        '\nRequested data was successfully scraped:\n'
        f'data set....: {scrape_config.DISPLAY_NAME}\n'
        f'date range..: {start_str} - {end_str}\n'
    )
    click.secho(success, fg='green')
    return 0


def __validate_date_range(session, start, end):
    if start.year != end.year:
        error = (
            "Start and end dates must both be in the same year and within "
            "the scope of that year's MLB Regular Season."
        )
        return dict(success=False, message=error)
    if start > end:
        start_str = start.strftime(DATE_ONLY)
        end_str = end.strftime(DATE_ONLY)
        error = (
            '"Start" must be before or the same date as "end":\n'
            f'start_date: {start_str}\n'
            f'end_date: {end_str}'
        )
        return dict(success=False, message=error)

    season = Season.find_by_year(session, start.year)
    start_date_valid = Season.is_date_in_season(session, start)['success']
    end_date_valid = Season.is_date_in_season(session, end)['success']
    if not start_date_valid or not end_date_valid:
        error = (
            f"Start and end date must both be within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}"
        )
        return dict(success=False, message=error)
    return dict(success=True)


def __get_driver(scrape_config):
    if not scrape_config.REQUIRES_SELENIUM:
        return dict(success=True, driver=None)
    result = get_chromedriver()
    if not result['success']:
        return result
    driver = result['result']
    return dict(success=True, driver=driver)


def __scrape_data_for_date(session, scrape_date, scrape_config, driver):
    input_dict = dict(date=scrape_date, session=session)
    if scrape_config.REQUIRES_INPUT:
        result = scrape_config.GET_INPUT_FUNCTION(scrape_date)
        if not result['success']:
            return result
        input_dict['input_data'] = result['result']
    if scrape_config.REQUIRES_SELENIUM:
        input_dict['driver'] = driver
    result = scrape_config.SCRAPE_FUNCTION(input_dict)
    if not result['success']:
        return result
    scraped_data = result['result']
    if scrape_config.PRODUCES_LIST:
        return __upload_scraped_data_list(scraped_data, scrape_date, scrape_config)
    return scrape_config.PERSIST_FUNCTION(scraped_data, scrape_date)


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
            result = scrape_config.PERSIST_FUNCTION(data, scrape_date)
            if not result['success']:
                return result
            delay_ms = (randint(50, 100)/100.0)
            time.sleep(delay_ms)
            pbar.update()
    return dict(success=True)

if __name__ == '__main__':
    cli({})