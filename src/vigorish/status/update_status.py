from halo import Halo

from vigorish.config.database import Season
from vigorish.status.update_status_bbref_boxscores import update_data_set_bbref_boxscores
from vigorish.status.update_status_bbref_games_for_date import update_data_set_bbref_games_for_date
from vigorish.status.update_status_brooks_games_for_date import (
    update_data_set_brooks_games_for_date,
)
from vigorish.status.update_status_brooks_pitch_logs import update_data_set_brooks_pitch_logs
from vigorish.status.update_status_brooks_pitchfx import update_data_set_brooks_pitchfx
from vigorish.util.result import Result


def update_status_for_mlb_season(scraped_data, session, year):
    spinner = Halo(text=f"Updating MLB {year}...", color="yellow", spinner="dots3")
    spinner.start()

    season = Season.find_by_year(session, year)
    if not season:
        spinner.stop_and_persist("😢", "Fail").clear()
        error = f"Error occurred retrieving season for year={year}"
        return Result.Fail(error)

    spinner.text = f"Updating MLB {year} bbref_games_for_date..."
    result = update_data_set_bbref_games_for_date(scraped_data, session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    session.commit()

    spinner.text = f"Updating MLB {year} brooks_games_for_date..."
    spinner.color = "white"
    result = update_data_set_brooks_games_for_date(scraped_data, session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    session.commit()

    spinner.text = f"Updating MLB {year} bbref_boxscores..."
    spinner.color = "green"
    result = update_data_set_bbref_boxscores(scraped_data, session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    session.commit()

    spinner.text = f"Updating MLB {year} brooks_pitch_logs..."
    spinner.color = "cyan"
    result = update_data_set_brooks_pitch_logs(scraped_data, session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    session.commit()

    spinner.text = f"Updating MLB {year} brooks_pitchfx..."
    spinner.color = "magenta"
    result = update_data_set_brooks_pitchfx(scraped_data, session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    session.commit()

    spinner.stop_and_persist("😎", "Success").clear()
    return Result.Ok()
