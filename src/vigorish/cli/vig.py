"""CLI application entry point."""
import os
import subprocess

import click
from getch import pause

from vigorish.app import Vigorish
from vigorish.cli.click_params import DataSetName, DateString, FileTypeName, JobName, MlbSeason
from vigorish.cli.components import print_message, validate_scrape_dates
from vigorish.cli.main_menu import MainMenu
from vigorish.config.project_paths import VIG_FOLDER
from vigorish.enums import DataSet, StatusReport, SyncDirection
from vigorish.scrape.job_runner import JobRunner
from vigorish.status.report_status import (
    report_date_range_status,
    report_season_status,
    report_status_single_date,
)
from vigorish.tasks import SyncDataNoPromptsTask
from vigorish.util.datetime_util import current_year, today_str
from vigorish.util.result import Result
from vigorish.util.string_helpers import flatten_list2d


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_context
def cli(ctx):
    """
    Vigorish scrapes various websites for MLB data.

    Please visit https://aaronluna.dev/projects/vigorish for user guides and project documentation.
    """
    if os.environ.get("ENV") != "TEST":  # pragma: no cover
        if not VIG_FOLDER.exists():
            VIG_FOLDER.mkdir()
    ctx.obj = Vigorish()


@cli.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_obj
def ui(app):  # pragma: no cover
    """Menu-driven UI powered by Bullet."""
    try:
        result = MainMenu(app).launch()
        return exit_app(app, result)
    except Exception as e:
        return exit_app(app, Result.Fail(f"Error: {repr(e)}"))


@cli.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.confirmation_option(prompt="Are you sure you want to delete all existing data?")
@click.pass_obj
def setup(app):  # pragma: no cover
    """Populate database with initial player, team and season data.

    WARNING! Before the setup process begins, all existing data will be deleted. This cannot be undone.
    """
    print()  # place an empty line between the command and the progress bars
    result = app.initialize_database()
    return exit_app(app, result, "Successfully populated database with initial data.")


@cli.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--data-set",
    type=DataSetName(),
    multiple=True,
    default=[str(DataSet.ALL)],
    show_default=True,
    help="Data set(s) to scrape, multiple values can be provided.",
)
@click.option(
    "--start",
    type=DateString(),
    prompt=True,
    help=("Date to start scraping data, string can be in any format that is recognized by dateutil.parser."),
)
@click.option(
    "--end",
    type=DateString(),
    prompt=True,
    help=("Date to stop scraping data, string can be in any format that is recognized by dateutil.parser."),
)
@click.option(
    "--job-name",
    type=JobName(),
    default="",
    help="A name to help identify this job.",
    prompt=("(Optional) Enter a name for this job (ONLY letters, numbers, underscore, and/or hyphen characters)"),
)
@click.pass_obj
def scrape(app, data_set, start, end, job_name):
    """Scrape MLB data from websites."""
    result = validate_scrape_dates(app.db_session, start, end)
    if result.failure:
        return exit_app(app, result)
    new_scrape_job = app.create_scrape_job(data_set, start, end, job_name).value
    job_runner = JobRunner(app=app, db_job=new_scrape_job)
    result = job_runner.execute()
    return exit_app(app, result)


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_obj
def status(app):
    """Report progress of scraped data, by date or MLB season."""
    pass


@status.command("date", context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("game_date", type=DateString(), default=today_str)
@click.option(
    "-v",
    "verbosity",
    count=True,
    default=1,
    help=(
        "Specify the level of detail to report:\n"
        "    -v: report the combined scrape progress for all games on the specified date.\n"
        "   -vv: report combined and individual scrape progress for each game on the specified date\n"
        "  -vvv: report combined/individual game scrape progress and pitch appearance scrape progress\n"
    ),
)
@click.pass_obj
def status_date(app, game_date, verbosity):
    """Report status for a single date.

    Dates can be provided in any format that is recognized by dateutil.parser.
    For example, all of the following strings are valid ways to represent the same date:
    "2018-5-13" -or- "05/13/2018" -or- "May 13 2018"
    """
    report_type = StatusReport.NONE
    if verbosity <= 0:
        error = f"Invalid value for verbosity: {verbosity}. Value must be greater than zero."
        return exit_app(app, Result.Fail(error))
    elif verbosity == 1:
        report_type = StatusReport.DATE_DETAIL_ALL_DATES
    elif verbosity == 2:
        report_type = StatusReport.DATE_DETAIL_MISSING_PITCHFX
    else:
        report_type = StatusReport.SINGLE_DATE_WITH_GAME_STATUS
    result = report_status_single_date(app.db_session, game_date, report_type)
    if result.success:
        report_viewer = result.value
        report_viewer.launch()
    return exit_app(app, result)


@status.command("range", context_settings={"help_option_names": ["-h", "--help"]})
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
    """Report status for each date in a specified range.

    Dates can be provided in any format that is recognized by dateutil.parser.
    For example, all of the following strings are valid ways to represent the same date:
    "2018-5-13" -or- "05/13/2018" -or- "May 13 2018"
    """
    report_type = StatusReport.NONE
    if verbosity <= 0:
        error = f"Invalid value for verbosity: {verbosity}. Value must be greater than zero."
        return exit_app(app, Result.Fail(error))
    elif verbosity == 1:
        report_type = StatusReport.DATE_SUMMARY_MISSING_DATA
    elif verbosity == 2:
        report_type = StatusReport.DATE_SUMMARY_ALL_DATES
    elif verbosity == 3:
        report_type = StatusReport.DATE_DETAIL_MISSING_DATA
    elif verbosity == 4:
        report_type = StatusReport.DATE_DETAIL_ALL_DATES
    else:
        report_type = StatusReport.DATE_DETAIL_MISSING_PITCHFX
    result = report_date_range_status(app.db_session, start, end, report_type)
    if result.success:
        report_viewer = result.value
        report_viewer.launch()
    return exit_app(app, result)


@status.command("season", context_settings={"help_option_names": ["-h", "--help"]})
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
        return exit_app(app, Result.Fail(error))
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
    else:
        report_type = StatusReport.DATE_DETAIL_MISSING_PITCHFX
    result = report_season_status(app.db_session, year, report_type)
    if result.success:
        report_viewer = result.value
        report_viewer.launch()
    return exit_app(app, result)


@cli.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.pass_obj
def sync(app):
    """Synchronize scraped data to/from S3 bucket."""
    pass


@sync.command("up", context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("year", type=MlbSeason(), default=current_year)
@click.option(
    "--file-type",
    type=FileTypeName(),
    help="Type of file to sync, must provide only one value.",
    prompt=True,
)
@click.option(
    "--data-set",
    type=DataSetName(),
    multiple=True,
    default=[str(DataSet.ALL)],
    show_default=True,
    help="Data set(s) to sync, multiple values can be provided.",
)
@click.pass_obj
def sync_up_to_s3(app, year, file_type, data_set):
    """Sync files from local folder to S3 bucket."""
    data_sets_int = sum(int(ds) for ds in flatten_list2d(data_set))
    result_dict = SyncDataNoPromptsTask(app).execute(
        sync_direction=SyncDirection.UP_TO_S3,
        year=year,
        file_type=file_type,
        data_sets_int=data_sets_int,
    )
    result = Result.Combine(list(result_dict.values()))
    if os.environ.get("ENV") != "TEST":  # pragma: no cover
        pause(message="\nPress any key to continue...")
    return exit_app(app, result)


@sync.command("down", context_settings={"help_option_names": ["-h", "--help"]})
@click.argument("year", type=MlbSeason(), default=current_year)
@click.option(
    "--file-type",
    type=FileTypeName(),
    help="Type of file to sync, must provide only one value.",
    prompt=True,
)
@click.option(
    "--data-set",
    type=DataSetName(),
    multiple=True,
    default=[str(DataSet.ALL)],
    show_default=True,
    help="Data set(s) to sync, multiple values can be provided.",
)
@click.pass_obj
def sync_down_to_local(app, year, file_type, data_set):
    """Sync files from S3 bucket to local folder."""
    data_sets_int = sum(int(ds) for ds in flatten_list2d(data_set))
    result_dict = SyncDataNoPromptsTask(app).execute(
        sync_direction=SyncDirection.DOWN_TO_LOCAL,
        year=year,
        file_type=file_type,
        data_sets_int=data_sets_int,
    )
    result = Result.Combine(list(result_dict.values()))
    if os.environ.get("ENV") != "TEST":  # pragma: no cover
        pause(message="\nPress any key to continue...")
    return exit_app(app, result)


def exit_app(app, result, message=None):
    app.db_session.close()
    subprocess.run(["clear"])
    print()
    exit_code = exit_app_success(message) if result.success else exit_app_error(result.error)
    print()
    return exit_code


def exit_app_success(message=None):
    if message:
        print_message(message, fg="bright_green", bold=True)
    return 0


def exit_app_error(message):
    if message:
        if isinstance(message, list):
            for m in message:
                print_message(m, fg="bright_red", bold=True)
        else:
            print_message(message, fg="bright_red", bold=True)
    return 1
