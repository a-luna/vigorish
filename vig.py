"""CLI application entry point."""
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main.constants import MLB_DATA_SETS, CLI_COLORS
from app.main.job import ScrapeJob
from app.main.models.base import Base
from app.main.models.season import Season
from app.main.models.status_date import DateScrapeStatus
from app.main.models.views.materialized_view import refresh_all_mat_views
from app.main.setup.initialize_database import initialize_database
from app.main.status.update_status import update_status_for_mlb_season
from app.main.util.click_params import DateString, MlbDataSet, MlbSeason
from app.main.util.datetime_util import get_date_range, today_str, current_year, format_timedelta
from app.main.util.dt_format_strings import DATE_ONLY, MONTH_NAME_SHORT
from app.main.util.result import Result

# TODO Create unit tests for all substitution parsing scenarios
# TODO Track lineup changes to avoid the various name,pos=N/A and lineupslot=0 hacks introduced in order to get boxscores parsing successfully
# TODO Create config file and config.example with settings for AWS auth, S3 bucket name/local folder path, DB URL, chrome/chromedriver binaries
# TODO Create vig config command which prompts user for values listed above and writes to config file.

DOTENV_PATH = Path.cwd() / ".env"

@click.group()
@click.pass_context
def cli(ctx):
    """Vigorish is a tool for scraping various MLB data sets from baseball-reference.com and
    brooksbaseball.com (e.g., detailed boxscores, pitchfx measurements, player biographical info).
    """
    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)
    engine = create_engine(os.getenv("DATABASE_URL"))
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    ctx.obj = {"engine": engine, "session": session}


@cli.command()
@click.confirmation_option(prompt="Are you sure you want to delete all existing data?")
@click.pass_obj
def setup(db):
    """Populate database with initial player, team and season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    Base.metadata.drop_all(db["engine"])
    Base.metadata.create_all(db["engine"])
    result = initialize_database(db["session"])
    if result.failure:
        return exit_app_error(db, result)
    refresh_all_mat_views(db)
    return exit_app_success(db, "Successfully populated database with initial data.")


@cli.command()
@click.option(
    "--data-set", type=MlbDataSet(), default="all", show_default=True, prompt=True,
    help= f'Data set to scrape, must be a value from the following list:\n{", ".join(MLB_DATA_SETS)}')
@click.option(
    "--start", type=DateString(), default=today_str, show_default=True, prompt=True,
    help="Date to start scraping data, string can be in any format that is recognized by dateutil.parser.")
@click.option(
    "--end", type=DateString(), default=today_str, show_default=True, prompt=True,
    help="Date to stop scraping data, string can be in any format that is recognized by dateutil.parser.")
@click.option(
    "--update/--no-update", default=True, show_default=True,
    help="Update statistics for scraped dates and games after scraping is complete.")
@click.pass_obj
def scrape(db, data_set, start, end, update):
    """Scrape MLB data from websites."""
    db['update_s3'] = update
    job = ScrapeJob(db, data_set, start, end)
    result = job.run()
    if result.failure:
        return exit_app_error(db, result)
    print_message(job.status_report, fg="green")

    season = Season.find_by_year(db['session'], start.year)
    result = refresh_season_data(db, season.year)
    if result.failure:
        return exit_app_error(db, result)
    print_message(season.status_report(), fg="bright_yellow")
    return exit_app_success(db)


@cli.group()
@click.option(
    "--update/--no-update", default=False, show_default=True,
    help="Update statistics for scraped dates and games before generating status report.")
@click.pass_obj
def status(db, update):
    """Report progress of scraped data, by date or MLB season."""
    db['update_s3'] = update


@status.command("date")
@click.argument("game_date", type=DateString(), default=today_str)
@click.pass_obj
def status_date(db, game_date):
    """Report status for a single date."""
    season = Season.find_by_year(db["session"], game_date.year)
    date_is_valid = Season.is_date_in_season(db["session"], game_date).success
    date_str = game_date.strftime(DATE_ONLY)
    if not date_is_valid:
        error = (
            f"'{date_str}' is not within the {season.name}:\n"
            f"season_start_date: {season.start_date_str}\n"
            f"season_end_date: {season.end_date_str}")
        return exit_app_error(db, error)
    result = refresh_season_data(db, season.year)
    if result.failure:
        return exit_app_error(db, result)
    date_status = DateScrapeStatus.find_by_date(db["session"], game_date)
    if not date_status:
        error = f"scrape_status_date does not contain an entry for date: {date_str}"
    click.secho(
        f"\n### STATUS REPORT FOR {game_date.strftime(MONTH_NAME_SHORT)} ###",
        fg="cyan",
        bold=True)
    click.secho(date_status.status_report(), fg="cyan")
    return exit_app_success(db)


@status.command("range")
@click.option("--start", type=DateString(), prompt=True, help="First date to report status.")
@click.option("--end", type=DateString(), prompt=True, help="Last date to report status.")
@click.pass_obj
def status_date_range(db, start, end):
    """Report overall status for each date in the specified range (includes both START and END dates).

    Dates can be provided in any format that is recognized by dateutil.parser.
    For example, all of the following strings are valid ways to represent the same date:
    "2018-5-13" -or- "05/13/2018" -or- "May 13 2018"
    """
    result = Season.validate_date_range(db["session"], start, end)
    if result.failure:
        return result
    season = result.value
    result = refresh_season_data(db, season.year)
    if result.failure:
        return exit_app_error(db, result)
    date_range = get_date_range(start, end)
    status_date_range = []
    for d in date_range:
        date_status = DateScrapeStatus.find_by_date(db["session"], d)
        if not date_status:
            error = f"scrape_status_date does not contain an entry for date: {d.strftime(DATE_ONLY)}"
        status_date_range.append(date_status)
    start_str = start.strftime(MONTH_NAME_SHORT)
    end_str = end.strftime(MONTH_NAME_SHORT)
    click.secho(f"\n### STATUS REPORT FOR {start_str} - {end_str} ###", fg="bright_magenta", bold=True)
    for status in status_date_range:
        date_str = status.game_date_str
        status_description = status.scrape_status_description
        click.secho(f"{date_str}: {status_description}", fg="bright_magenta")
    return exit_app_success(db)


@status.command("season")
@click.argument("year", type=MlbSeason(), default=current_year)
@click.pass_obj
def status_season(db, year):
    """Report status for a single MLB season."""
    season = Season.find_by_year(db["session"], year)
    result = refresh_season_data(db, season.year)
    if result.failure:
        return exit_app_error(db, result)
    click.secho(f"\n### STATUS REPORT FOR {season.name} ###", fg="bright_yellow", bold=True)
    click.secho(season.status_report(), fg="bright_yellow")
    return exit_app_success(db)


def refresh_season_data(db, year):
    if db['update_s3']:
        result = update_status_for_mlb_season(db['session'], year)
        if result.failure:
            return result
    refresh_all_mat_views(db)
    return Result.Ok()


def exit_app_success(db, message=None):
    if message:
        print_message(message, fg="green")
    db['session'].close()
    return 0


def exit_app_error(db, result):
    print_message(result.error, fg="red")
    db['session'].close()
    return 1


def print_message(message, fg=None,  bg=None, bold=None, underline=None):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    click.secho(f"{message}\n", fg=fg, bg=bg, bold=bold, underline=underline)


if __name__ == "__main__":
    cli({})
