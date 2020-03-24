"""CLI application entry point."""
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.cli.menus.main_menu import MainMenu
from vigorish.cli.util import print_message
from vigorish.config.database import get_db_url, initialize_database, Base
from vigorish.config.types import ConfigFile
from vigorish.constants import CLI_COLORS
from vigorish.data.scraped_data import ScrapedData
from vigorish.scrape.url_builder import UrlBuilder


APP_FOLDER = Path(__file__).parent.parent
APP_ROOT = APP_FOLDER.parent.parent
CONFIG = APP_ROOT / "vig.config.json"
DOTENV = APP_ROOT / ".env"


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
    url_builder = UrlBuilder(config=config, scraped_data=scraped_data)
    ctx.obj = {
        "config": config,
        "engine": engine,
        "session": session,
        "scraped_data": scraped_data,
        "url_builder": url_builder,
    }


@cli.command()
@click.pass_obj
def ui(vig):
    """Menu-driven TUI powered by Bullet."""
    try:
        main_menu = MainMenu(vig)
        result = main_menu.launch()
        return exit_app_success() if result.success else exit_app_error(result.error)
    except Exception as e:
        return exit_app_error(f"Error: {repr(e)}")


@cli.command()
@click.confirmation_option(prompt="Are you sure you want to delete all existing data?")
@click.pass_obj
def setup(vig):
    """Populate database with initial player, team and season data.

    WARNING! Before the setup process begins, all existing data will be
    deleted. This cannot be undone.
    """
    print()  # place an empty line between the command and the progress bars
    result = initialize_database()
    if result.failure:
        return exit_app_error(vig, result)
    return exit_app_success(vig, "Successfully populated database with initial data.")


def exit_app_success(message=None):
    if message:
        print_message(f"{message}\n", fg="bright_green", bold=True)
    return 0


def exit_app_error(message):
    print_message(f"{message}\n", fg="bright_red", bold=True)
    return 1
