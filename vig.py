"""CLI application entry point."""
import os
from dateutil import parser

import click
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

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

SQLALCHEMY_DATABASE_URL = 'postgresql://localhost.localdomain/vig_dev'
engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(bind=engine)


class DateString(click.ParamType):
    name = 'date-string'
    def convert(self, value, param, ctx):
        try:
            date = parser.parse(value)
            return date
        except Exception:
            error = (
                f'"{value}" could not be parsed as a valid date. You can use '
                'any format recognized by dateutil.parser, for example:'
                '\t2018-5-13  -or-  08/10/2017  -or-  "Apr 27 2018"'
            )
            self.fail(error, param, ctx)


@click.group()
def cli():
    pass


@cli.command()
@click.confirmation_option(
    prompt='\nAre you sure you want to delete all existing data?')
def setup():
    """Populate database with initial Player, Team and MLB Season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    s = Session()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

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

if __name__ == '__main__':
    cli()