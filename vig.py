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

from config import config_by_name
from app.main.constants import MLB_DATA_SETS
from app.main.data.scrape.brooks.scrape_brooks_games_for_date import (
    scrape_brooks_games_for_date
)
from app.main.data.setup.populate_base_tables import populate_base_tables
from app.main.data.setup.populate_seasons import populate_seasons
from app.main.data.setup.populate_players import populate_players
from app.main.data.setup.populate_teams import populate_teams
from app.main.data.setup.truncate_tables import delete_all_data
from app.main.models.base import Base
from app.main.models.player import Player
from app.main.models.player_id import PlayerId
from app.main.models.runners_on_base import RunnersOnBase
from app.main.models.season import Season
from app.main.models.team import Team
from app.main.util.datetime_util import get_date_range
from app.main.util.dt_format_strings import DATE_ONLY
from app.main.util.s3_helper import (
    upload_brooks_games_for_date, get_brooks_games_for_date_from_s3
)

#TODO New tables: SeasonScrapeStatus, DayScrapeStatus, GameScrapeStatus, PitchAppScrapeStatus, PlayerScrapeStatus
#TODO New setup processes: create entries in season and day scrapestatus tables.
#TODO New cli command: vig status [YEAR], reports scrape status of mlb reg season for year provided 

APP_ROOT = Path.cwd()
DOTENV_PATH = APP_ROOT / '.env'
GET_INPUT_FUNCTIONS = dict(
    brooks_pitch_logs=get_brooks_games_for_date_from_s3
)
SCRAPE_FUNCTIONS = dict(
    brooks_games_for_date=scrape_brooks_games_for_date
)
SAVE_RESULT_FUNCTIONS = dict(
    brooks_games_for_date=upload_brooks_games_for_date
)

class DateString(click.ParamType):
    name = 'date-string'
    def convert(self, value, param, ctx):
        try:
            date = parser.parse(value)
            return date
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can use '
                'any format recognized by dateutil.parser, for example: '
                '2018-5-13  -or-  08/10/2017  -or-  "Apr 27 2018"'
            )
            self.fail(error, param, ctx)


@click.group()
@click.pass_context
def cli(ctx):
    if DOTENV_PATH.is_file():
        load_dotenv(DOTENV_PATH)

    engine = create_engine(os.getenv('DATABASE_URL'))
    session = sessionmaker(bind=engine)
    ctx.obj = {
        'engine': engine,
        'session': session
    }


@cli.command()
@click.confirmation_option(
    prompt='\nAre you sure you want to delete all existing data?')
@click.pass_context
def setup(ctx):
    """Populate database with initial Player, Team and MLB Season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    s = ctx.obj['session']()
    Base.metadata.drop_all(ctx.obj['engine'])
    Base.metadata.create_all(ctx.obj['engine'])

    result = populate_base_tables(s)
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        s.close()
        return 1

    result = populate_seasons(s)
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        s.close()
        return 1

    result = populate_players(s)
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        s.close()
        return 1

    result = populate_teams(s)
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        s.close()
        return 1

    click.secho('\nSuccessfully populated database with initial data.\n', fg='green')
    s.close()
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
    s = ctx.obj['session']()
    result = __validate_date_range(s, start, end)
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        s.close()
        return 1

    func_dict = __get_func_dict_for_data_set(data_set)
    date_range = get_date_range(start, end)
    with tqdm(total=len(date_range), ncols=100, unit='page') as pbar:
        for scrape_date in date_range:
            pbar.set_description(f'Processing: {scrape_date.strftime(DATE_ONLY)}')
            result = __scrape_data_for_date(s, scrape_date, func_dict)
            if not result['success']:
                break
            delay_ms = (randint(150, 250)/100.0)
            time.sleep(delay_ms)
            pbar.update()

    s.close()
    if not result['success']:
        click.secho(f"\n{result['message']}\n", fg='red')
        return 1
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


def __get_func_dict_for_data_set(data_set):
    get_input_function = GET_INPUT_FUNCTIONS[data_set] \
        if data_set in GET_INPUT_FUNCTIONS else None
    return dict(
        get_input=get_input_function,
        scrape=SCRAPE_FUNCTIONS[data_set],
        upload=SAVE_RESULT_FUNCTIONS[data_set]
    )


def __scrape_data_for_date(s, scrape_date, func_dict):
    get_input_func = func_dict['get_input']
    input_data = get_input_func(scrape_date) \
        if get_input_func else None
    input_dict = dict(date=scrape_date, input_data=input_data, session=s)
    result = func_dict['scrape'](input_dict)
    if not result['success']:
        return result
    scraped_data = result['result']
    return func_dict['upload'](scraped_data, scrape_date)

if __name__ == '__main__':
    cli()