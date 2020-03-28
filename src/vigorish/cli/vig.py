"""CLI application entry point."""
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.cli.click_params import DateString, MlbSeason
from vigorish.cli.menus.main_menu import MainMenu
from vigorish.cli.util import print_message
from vigorish.config.database import get_db_url, initialize_database, Base, Season
from vigorish.config.types import ConfigFile
from vigorish.constants import CLI_COLORS
from vigorish.data.scraped_data import ScrapedData
from vigorish.enums import StatusReport
from vigorish.status.report_status import (
    report_status_single_date,
    report_date_range_status,
    report_season_status,
)
from vigorish.util.datetime_util import today_str, current_year

APP_FOLDER = Path(__file__).parent.parent
CONFIG = APP_FOLDER / "vig.config.json"
DOTENV = APP_FOLDER / ".env"


@click.group()
@click.pass_context
def cli(ctx):
    """Entry point for the CLI application."""
    if DOTENV.is_file():
        load_dotenv(DOTENV)
    config = ConfigFile(config_file_path=CONFIG)
    engine = create_engine(get_db_url())
    session_maker = sessionmaker(bind=engine)
    session = session_maker()
    scraped_data = ScrapedData(db=session, config=config)
    ctx.obj = {
        "config": config,
        "engine": engine,
        "session": session,
        "scraped_data": scraped_data,
    }


@cli.command()
@click.pass_obj
def ui(app):
    """Menu-driven TUI powered by Bullet."""
    try:
        main_menu = MainMenu(app)
        result = main_menu.launch()
        return exit_app_success(app) if result.success else exit_app_error(app, result.error)
    except Exception as e:
        return exit_app_error(app, f"Error: {repr(e)}")


@cli.command()
@click.confirmation_option(prompt="Are you sure you want to delete all existing data?")
@click.pass_obj
def setup(app):
    """Populate database with initial player, team and season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    print()  # place an empty line between the command and the progress bars
    result = initialize_database()
    if result.failure:
        return exit_app_error(app, result.error)
    return exit_app_success(app, "Successfully populated database with initial data.")


@cli.group()
@click.option(
    "--update/--no-update",
    default=False,
    show_default=True,
    help="Update statistics for scraped dates and games before generating status report.",
)
@click.pass_obj
def status(app, update):
    """Report progress of scraped data, by date or MLB season."""
    app["run_update"] = update


@status.command("date")
@click.argument("game_date", type=DateString(), default=today_str)
@click.option(
    "--missing-ids/--no-missing-ids",
    default=False,
    show_default=True,
    help="Report includes pitch_app_ids that have not been scraped.",
)
@click.option(
    "--with-games/--without-games",
    default=False,
    show_default=True,
    help="Report includes scrape statistics for all games on the specified date.",
)
@click.pass_obj
def status_date(app, game_date, missing_pitchfx, with_games):
    """Report status for a single date."""
    result = report_status_single_date(
        app["session"],
        app["scraped_data"],
        app["run_update"],
        game_date,
        missing_pitchfx,
        with_games,
    )
    return exit_app_success(app) if result.success else exit_app_error(app, result.error)


@status.command("range")
@click.option("--start", type=DateString(), prompt=True, help="First date to report status.")
@click.option("--end", type=DateString(), prompt=True, help="Last date to report status.")
@click.option(
    "-v",
    "verbosity",
    count=True,
    default=1,
    help=(
        "Specify the level of detail to report:\n"
        "    -v: summary report of only dates missing data\n"
        "   -vv: summary report of all dates\n"
        "  -vvv: detailed report of only dates missing data\n"
        " -vvvv: detailed report of all dates\n"
        "-vvvvv: detailed report of all dates with missing pitch_app_ids"
    ),
)
@click.pass_obj
def status_date_range(app, start, end, verbosity):
    """Report overall status for each date in the specified range (includes both START and END dates).

    Dates can be provided in any format that is recognized by dateutil.parser.
    For example, all of the following strings are valid ways to represent the same date:
    "2018-5-13" -or- "05/13/2018" -or- "May 13 2018"
    """
    report_type = StatusReport.NONE
    if verbosity <= 0:
        error = f"Invalid value for verbosity: {verbosity}. Value must be greater than zero."
        return exit_app_error(app, error)
    elif verbosity == 1:
        report_type = StatusReport.DATE_SUMMARY_MISSING_DATA
    elif verbosity == 2:
        report_type = StatusReport.DATE_SUMMARY_ALL_DATES
    elif verbosity == 3:
        report_type = StatusReport.DATE_DETAIL_MISSING_DATA
    elif verbosity == 4:
        report_type = StatusReport.DATE_DETAIL_ALL_DATES
    elif verbosity > 4:
        report_type = StatusReport.DATE_DETAIL_MISSING_PITCHFX
    else:
        error = "Unknown error occurred, unable to display status report."
        return exit_app_error(app, error)
    result = report_date_range_status(
        app["session"], app["scraped_data"], app["run_update"], start, end, report_type
    )
    return exit_app_success(app) if result.success else exit_app_error(app, result.error)


@status.command("season")
@click.argument("year", type=MlbSeason(), default=current_year)
@click.option(
    "-v",
    "verbosity",
    count=True,
    default=1,
    help=(
        "Specify the level of detail to report:"
        "     -v: overall metrics for entire season"
        "    -vv: summary report of dates in season that are missing data"
        "   -vvv: summary report of all dates in season"
        "  -vvvv: detailed report of dates in season that are missing data"
        "  -vvvv: detailed report of all dates in season"
        "-vvvvvv: detailed report of all dates in season with missing pitch_app_ids"
    ),
)
@click.pass_obj
def status_season(app, year, verbosity):
    """Report status for a single MLB season."""
    report_type = StatusReport.NONE
    if verbosity <= 0:
        error = f"Invalid value for verbosity: {verbosity}. Value must be greater than zero."
        return exit_app_error(app, error)
    elif verbosity == 1:
        report_type = StatusReport.SEASON_SUMMARY
    elif verbosity == 2:
        report_type = StatusReport.DATE_SUMMARY_MISSING_DATA
    elif verbosity == 3:
        report_type = StatusReport.DATE_SUMMARY_ALL_DATES
    elif verbosity == 4:
        report_type = StatusReport.DATE_DETAIL_MISSING_DATA
    elif verbosity == 5:
        report_type = StatusReport.DATE_DETAIL_ALL_DATES
    elif verbosity > 5:
        report_type = StatusReport.DATE_DETAIL_MISSING_PITCHFX
    else:
        error = "Unknown error occurred, unable to display status report."
        return exit_app_error(app, error)
    result = report_season_status(
        app["session"], app["scraped_data"], app["run_update"], year, report_type
    )
    return exit_app_success(app) if result.success else exit_app_error(app, result.error)


def exit_app_success(app, message=None):
    if message:
        print_message(f"{message}\n", fg="bright_green", bold=True)
    app["session"].close()
    return 0


def exit_app_error(app, message):
    print_message(f"{message}\n", fg="bright_red", bold=True)
    app["session"].close()
    return 1
