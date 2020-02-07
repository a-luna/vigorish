"""CLI application entry point."""
import os
from pathlib import Path

import click
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from vigorish.cli.menus.main_menu import MainMenu
from vigorish.config.file import get_config_from_file
from vigorish.constants import CLI_COLORS

APP_FOLDER = Path(__file__).parent.parent
APP_ROOT = APP_FOLDER.parent.parent
SQLITE_DEV = "sqlite:///" + str(APP_FOLDER / "vig_dev.db")
SQLITE_PROD = "sqlite:///" + str(APP_FOLDER / "vig_prod.db")
DOTENV = APP_ROOT / ".env"


@click.group()
@click.pass_context
def cli(ctx):
    """Entry point for the CLI application."""
    if DOTENV.is_file():
        load_dotenv(DOTENV)
    result = get_config_from_file()
    if result.failure:
        return exit_app_error(result.error)
    config = result.value
    engine = create_engine(get_db_url())
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    ctx.obj = {"config": config, "engine": engine, "session": session}


@cli.command()
@click.pass_obj
def ui(vig):
    """Menu-driven UI for vigorish."""
    try:
        main_menu = MainMenu(vig)
        result = main_menu.launch()
        return exit_app_success() if result.success else exit_app_error(result.error)
    except Exception as e:
        return exit_app_error(f"Error: {repr(e)}")


def get_db_url():
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        env = os.getenv("ENV", "dev")
        if env == "prod":
            return SQLITE_PROD
        return SQLITE_DEV
    return db_url


def exit_app_success(message=None):
    if message:
        print_message(message, fg="green")
    return 0


def exit_app_error(message):
    print_message(message, fg="red")
    return 1


def print_message(message, fg=None, bg=None, bold=None, underline=None):
    if (fg and fg not in CLI_COLORS) or (bg and bg not in CLI_COLORS):
        fg = None
        bg = None
    click.secho(f"{message}\n", fg=fg, bg=bg, bold=bold, underline=underline)
