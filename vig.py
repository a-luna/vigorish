"""CLI application entry point."""
import os
import time
from pathlib import Path
from random import randint

import click
from dateutil import parser
from dotenv import load_dotenv
from halo import Halo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from app.main.config import scrape_config_by_data_set
from app.main.constants import MLB_DATA_SETS
from app.main.models.base import Base
from app.main.models.season import Season
from app.main.models.views.materialized_view import refresh_all_mat_views
from app.main.setup.initialize_database import initialize_database
from app.main.status.update_status import update_status_for_mlb_season
from app.main.util.click_params import DateString, MlbSeason, MlbDataSet
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY, MONTH_NAME_SHORT
from app.main.util.list_functions import print_list
from app.main.util.result import Result
from app.main.util.scrape_functions import get_chromedriver

#TODO Create unit tests for all substitution parsing scenarios
#TODO Track lineup changes to avoid the various name,pos=N/A and lineupslot=0 hacks introduced in order to get boxscores parsing successfully
#TODO Create config file and config.example with settings for AWS auth, S3 bucket name/local folder path, DB URL, chrome/chromedriver binaries
#TODO Create vig config command which prompts user for values listed above and writes to config file.

APP_ROOT = Path.cwd()
DOTENV_PATH = APP_ROOT / '.env'


@click.group()
@click.pass_context
def cli(ctx):
    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)

    engine = create_engine(os.getenv('DATABASE_URL'))
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    ctx.obj = {
        'engine': engine,
        'session': session
    }

def clean():
    """Remove *.pyc and *.pyo files recursively starting at current directory."""
    for dirpath, _, filenames in os.walk('.'):
        for filename in filenames:
            if filename.endswith('.pyc') or filename.endswith('.pyo'):
                full_pathname = os.path.join(dirpath, filename)
                click.echo('Removing {}'.format(full_pathname))
                os.remove(full_pathname)


@cli.command()
@click.confirmation_option(
    prompt='Are you sure you want to delete all existing data?')
@click.pass_context
def setup(ctx):
    """Populate database with initial Player, Team and Season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    engine = ctx.obj['engine']
    session = ctx.obj['session']
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    result = initialize_database(session)
    if result.failure:
        click.secho(str(result), fg='red')
        session.close()
        return 1
    refresh_all_mat_views(engine, session)
    click.secho('Successfully populated database with initial data.\n', fg='green')
    session.close()
    return 0


@cli.command()
@click.option(
    '--data-set',
    type=MlbDataSet(),
    prompt=True,
    help=(
        'Data set to scrape, must be a value from the following list:\n'
        f'{print_list(MLB_DATA_SETS)}'
    )
)
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
    """Scrape MLB data from websites."""
    engine = ctx.obj['engine']
    session = ctx.obj['session']
    result = __validate_date_range(session, start, end)
    if result.failure:
        click.secho(str(result), fg='red')
        session.close()
        return 1

    scrape_config = scrape_config_by_data_set[data_set]
    result = __get_driver(scrape_config)
    if result.failure:
        click.secho(str(result), fg='red')
        session.close()
        return 1
    driver = result.value

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
            if result.failure:
                break

            session.commit()
            refresh_all_mat_views(engine, session)
            time.sleep(randint(250, 300)/100.0)
            pbar.update()

    session.close()
    if scrape_config.requires_selenium:
        driver.close()
        driver.quit()
    if result.failure:
        click.secho(str(result), fg='red')
        return 1
    start_str = start.strftime(MONTH_NAME_SHORT)
    end_str = end.strftime(MONTH_NAME_SHORT)
    success = (
        'Requested data was successfully scraped:\n'
        f'data set....: {scrape_config.display_name}\n'
        f'date range..: {start_str} - {end_str}\n'
    )
    click.secho(success, fg='green')
    return 0

@cli.command()
@click.option(
    '--year',
    type=MlbSeason(),
    prompt=True,
    help=(
        'Year of the MLB Season to report scrape progress.'))
@click.option(
    '--refresh/--no-refresh',
    default=False,
    help=(
        'Determines if all scraped data is examined before producing status '
        'report. By default, data is not refreshed. Refresh process requires '
        'approximately 25 minutes to complete.'
    )
)
@click.pass_context
def status(ctx, year, refresh):
    """Report progress of scraped mlb data sets."""
    engine = ctx.obj['engine']
    session = ctx.obj['session']
    spinner = Halo(text='Loading', spinner='noise')
    spinner_started = False

    if refresh:
        result = update_status_for_mlb_season(session, year)
        if result.failure:
            click.secho(str(result), fg='red')
            return 1
    else:
        spinner_started = True
        spinner.start()
    refresh_all_mat_views(engine, session)
    if spinner_started:
        spinner.stop()

    mlb = Season.find_by_year(session, year)
    print(mlb.status_report())
    session.close()
    return 0

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


def __get_driver(scrape_config):
    if not scrape_config.requires_selenium:
        return Result.Ok()
    return get_chromedriver()


def __scrape_data_for_date(session, scrape_date, scrape_config, driver):
    input_dict = dict(date=scrape_date, session=session)
    if scrape_config.requires_input:
        result = scrape_config.get_input_function(scrape_date)
        if result.failure:
            return result
        input_dict['input_data'] = result.value

    if scrape_config.requires_selenium:
        input_dict['driver'] = driver
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
            delay_ms = (randint(50, 100)/100.0)
            time.sleep(delay_ms)
            pbar.update()
    return Result.Ok()

if __name__ == '__main__':
    cli({})
