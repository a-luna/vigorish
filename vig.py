"""CLI application entry point."""
import os
import time
from pathlib import Path
from random import randint

import click
from dateutil import parser
from halo import Halo
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tqdm import tqdm

from app.main.config import get_config_list
from app.main.constants import MLB_DATA_SETS, CLI_COLORS
from app.main.models.base import Base
from app.main.models.season import Season
from app.main.models.views.materialized_view import refresh_all_mat_views
from app.main.setup.initialize_database import initialize_database
from app.main.status.update_status import update_status_for_mlb_season
from app.main.util.click_params import DateString, MlbDataSet, MlbSeason
from app.main.util.decorators import measure_time
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY, MONTH_NAME_SHORT
from app.main.util.list_functions import print_list
from app.main.util.result import Result
from app.main.util.scrape_functions import get_chromedriver

# TODO Create unit tests for all substitution parsing scenarios
# TODO Track lineup changes to avoid the various name,pos=N/A and lineupslot=0 hacks introduced in order to get boxscores parsing successfully
# TODO Create config file and config.example with settings for AWS auth, S3 bucket name/local folder path, DB URL, chrome/chromedriver binaries
# TODO Create vig config command which prompts user for values listed above and writes to config file.


@click.group()
@click.pass_context
def cli(ctx):
    """Web scraper for various MLB data sets, including detailed boxscores, pitchfx measurements
    and player biographical info.
    """
    engine = create_engine(os.getenv("DATABASE_URL"))
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    ctx.obj = {"engine": engine, "session": session}


def clean():
    """Remove *.pyc and *.pyo files recursively starting at current directory."""
    for dirpath, _, filenames in os.walk("."):
        for filename in filenames:
            if filename.endswith(".pyc") or filename.endswith(".pyo"):
                full_pathname = os.path.join(dirpath, filename)
                click.echo(f"Removing {full_pathname}")
                os.remove(full_pathname)


@cli.command()
@click.confirmation_option(prompt="Are you sure you want to delete all existing data?")
@click.pass_obj
def setup(db):
    """Populate database with initial Player, Team and Season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    engine = db["engine"]
    session = db["session"]
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    result = initialize_database(session)
    if result.failure:
        return exit_app_error(session, result)
    refresh_all_mat_views(engine, session)
    return exit_app_success(session, "Successfully populated database with initial data.")


@cli.command()
@click.option(
    "-d",
    "--data-set",
    type=MlbDataSet(),
    default="all",
    show_default=True,
    prompt=True,
    help=(
        f'Data set to scrape, must be a value from the following list:\n{", ".join(MLB_DATA_SETS)}'
    ),
)
@click.option(
    "-s",
    "--start",
    "start_date",
    type=DateString(),
    prompt=True,
    help="Date to start scraping data, string can be in any format that is recognized by dateutil.parser."
)
@click.option(
    "-e",
    "--end",
    "end_date",
    type=DateString(),
    prompt=True,
    help="Date to stop scraping data, string can be in any format that is recognized by dateutil.parser."
)
@click.option(
    "--update/--no-update",
    default=False,
    show_default=True,
    help="Update statistics for scraped dates and games after scraping is complete."
)
@click.pass_obj
def scrape(db, data_set, start_date, end_date, update):
    """Scrape MLB data from websites."""
    engine = db["engine"]
    session = db["session"]
    result = get_prerequisites(session, data_set, start_date, end_date)
    if result.failure:
        return exit_app_error(session, result)
    (date_range, driver, config_list) = result.value

    with tqdm(
        total=len(date_range),
        unit="day",
        mininterval=0.12,
        maxinterval=5,
        position=0,
        leave=False,
        disable=True
    ) as pbar_date:
        for scrape_date in date_range:
            pbar_date.set_description(get_pbar_date_description(scrape_date))
            with tqdm(
                total=len(config_list),
                unit="data-set",
                mininterval=0.12,
                maxinterval=5,
                position=1,
                leave=False,
                disable=True
            ) as pbar_data_set:
                for config in config_list:
                    pbar_data_set.set_description(get_pbar_data_set_description(config.key_name))
                    result = scrape_data_for_date(session, scrape_date, driver, config)
                    if result.failure:
                        break
                    time.sleep(randint(250, 300) / 100.0)
                    pbar_data_set.update()
            pbar_date.update()

    driver.close()
    driver.quit()
    driver = None
    if result.failure:
        return exit_app_error(session, result)
    start_str = start_date.strftime(MONTH_NAME_SHORT)
    end_str = end_date.strftime(MONTH_NAME_SHORT)
    success = (
        "Requested data was successfully scraped:\n"
        f"data set....: {data_set}\n"
        f"date range..: {start_str} - {end_str}"
    )
    return exit_app_success(session, success)


@cli.command()
@click.option(
    "--year",
    type=MlbSeason(),
    prompt=True,
    help=("Year of MLB Season to report progress of scraped data sets."),
)
@click.pass_obj
def status(db, year):
    """Report progress (per-season) of scraped mlb data sets."""
    engine = db["engine"]
    session = db["session"]
    result = update_status_for_mlb_season(session, year)
    if result.failure:
        return exit_app_error(session, result)
    refresh_all_mat_views(engine, session)
    mlb = Season.find_by_year(session, year)
    print_message(mlb.status_report(), fg="cyan")
    return exit_app_success(session)


def get_prerequisites(session, data_set, start_date, end_date):
    result = validate_date_range(session, start_date, end_date)
    if result.failure:
        return result
    date_range = get_date_range(start_date, end_date)

    result = get_chromedriver()
    if result.failure:
        return result
    driver = result.value

    result = get_config_list(data_set)
    if result.failure:
        return result
    config_list = result.value

    return Result.Ok((date_range, driver, config_list))


def validate_date_range(session, start, end):
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
            f"start: {start_str}\n"
            f"end: {end_str}"
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
    return Result.Ok(season)


@measure_time
def scrape_data_for_date(session, scrape_date, driver, config):
    input_dict = dict(date=scrape_date, session=session)
    if config.requires_input:
        result = config.get_input_function(scrape_date)
        if result.failure:
            return result
        input_dict["input_data"] = result.value
    if config.requires_selenium:
        input_dict["driver"] = driver
    result = config.scrape_function(input_dict)
    if result.failure:
        return result
    scraped_data = result.value
    if config.produces_list:
        result = upload_scraped_data_list(scraped_data, scrape_date, config)
    else:
        result = config.persist_function(scraped_data, scrape_date)
    return result


def upload_scraped_data_list(scraped_data, scrape_date, config):
    with tqdm(
        total=len(scraped_data),
        unit="file",
        mininterval=0.12,
        maxinterval=5,
        leave=False,
        position=2,
        disable=True
    ) as pbar:
        for data in scraped_data:
            pbar.set_description(get_pbar_upload_description(data.upload_id))
            result = config.persist_function(data, scrape_date)
            if result.failure:
                return result
            time.sleep(randint(50, 100) / 100.0)
            pbar.update()
    return Result.Ok()


def exit_app_success(session, message=None):
    if message:
        print_message(message, fg="green")
    session.close()
    return 0


def exit_app_error(session, result):
    print_message(str(result), fg="red")
    session.close()
    return 1


def print_message(message, fg=None,  bg=None, bold=None, underline=None, blink=None):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    click.secho(f"{message}\n", fg=fg, bg=bg, bold=bold, underline=underline, blink=blink)


def get_pbar_date_description(date, req_len=32):
    pre =f"Processing {date.strftime(MONTH_NAME_SHORT)}"
    pad_len = req_len - len(pre)
    return f"{pre}{'.'*pad_len}"


def get_pbar_data_set_description(config_name, req_len=32):
    pre = f"Data Set {config_name}"
    pad_len = req_len - len(pre)
    return f"{pre}{'.'*pad_len}"


def get_pbar_upload_description(game_id, req_len=32):
    pre = f"Uploading {game_id}"
    pad_len = req_len - len(pre)
    return f"{pre}{'.'*pad_len}"


if __name__ == "__main__":
    cli({})
