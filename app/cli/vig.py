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

from app.cli.vig_scrape import scrape as scrape_command
from app.main.constants import MLB_DATA_SETS
from app.main.models.base import Base
from app.main.models.season import Season
from app.main.models.views.materialized_view import refresh_all_mat_views
from app.main.setup.initialize_database import initialize_database
from app.main.status.update_status import update_status_for_mlb_season
from app.main.util.click_params import DateString, MlbDataSet, MlbSeason
from app.main.util.datetime_util import get_date_range
from app.main.util.list_functions import print_list
from app.main.util.result import Result

#TODO Create unit tests for all substitution parsing scenarios
#TODO Track lineup changes to avoid the various name,pos=N/A and lineupslot=0 hacks introduced in order to get boxscores parsing successfully
#TODO Create config file and config.example with settings for AWS auth, S3 bucket name/local folder path, DB URL, chrome/chromedriver binaries
#TODO Create vig config command which prompts user for values listed above and writes to config file.

DOTENV_PATH = Path.cwd() / '.env'


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
        f'{", ".join(MLB_DATA_SETS)}'))
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
    session = ctx.obj['session']
    result = scrape_command(ctx, data_set, start, end)
    if result.failure:
        click.secho(str(result), fg='red')
        session.close()
        return 1
    return 0


@cli.command()
@click.option(
    '--year',
    type=MlbSeason(),
    prompt=True,
    help=(
        'Year of the MLB Season to report progress of scraped data sets.'))
@click.pass_context
def status(ctx, year):
    """Report progress of scraped mlb data sets."""
    engine = ctx.obj['engine']
    session = ctx.obj['session']
    spinner = Halo(text='Updating...', color='yellow', spinner='dots3')
    spinner.start()

    result = update_status_for_mlb_season(session, year)
    if result.failure:
        click.secho(str(result), fg='red')
        return 1
    refresh_all_mat_views(engine, session)
    spinner.stop()

    mlb = Season.find_by_year(session, year)
    print(mlb.status_report())
    session.close()
    return 0

if __name__ == '__main__':
    cli({})
