from halo import Halo

from vigorish.config.database import Season
from vigorish.constants import JOB_SPINNER_COLORS
from vigorish.enums import DataSet
from vigorish.status.update_status_bbref_boxscores import update_data_set_bbref_boxscores
from vigorish.status.update_status_bbref_games_for_date import update_data_set_bbref_games_for_date
from vigorish.status.update_status_brooks_games_for_date import (
    update_data_set_brooks_games_for_date,
)
from vigorish.status.update_status_brooks_pitch_logs import update_data_set_brooks_pitch_logs
from vigorish.status.update_status_brooks_pitchfx import update_data_set_brooks_pitchfx
from vigorish.util.result import Result


def update_status_for_mlb_season(db_session, scraped_data, year):
    spinner = Halo(color=JOB_SPINNER_COLORS[DataSet.BBREF_GAMES_FOR_DATE], spinner="dots3")
    spinner.text = f"Updating MLB {year}..."
    spinner.start()

    season = Season.find_by_year(db_session, year)
    if not season:
        spinner.stop_and_persist("😢", "Fail").clear()
        error = f"Error occurred retrieving season for year={year}"
        return Result.Fail(error)

    spinner.text = f"Updating MLB {year} bbref_games_for_date..."
    result = update_data_set_bbref_games_for_date(scraped_data, db_session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    db_session.commit()

    spinner.text = f"Updating MLB {year} brooks_games_for_date..."
    spinner.color = JOB_SPINNER_COLORS[DataSet.BROOKS_GAMES_FOR_DATE]
    result = update_data_set_brooks_games_for_date(scraped_data, db_session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    db_session.commit()

    spinner.text = f"Updating MLB {year} bbref_boxscores..."
    spinner.color = JOB_SPINNER_COLORS[DataSet.BBREF_BOXSCORES]
    result = update_data_set_bbref_boxscores(scraped_data, db_session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    db_session.commit()

    spinner.text = f"Updating MLB {year} brooks_pitch_logs..."
    spinner.color = JOB_SPINNER_COLORS[DataSet.BROOKS_PITCH_LOGS]
    result = update_data_set_brooks_pitch_logs(scraped_data, db_session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    db_session.commit()

    spinner.text = f"Updating MLB {year} brooks_pitchfx..."
    spinner.color = JOB_SPINNER_COLORS[DataSet.BROOKS_PITCHFX]
    result = update_data_set_brooks_pitchfx(scraped_data, db_session, season)
    if result.failure:
        spinner.stop_and_persist("😢", "Fail").clear()
        return result
    db_session.commit()

    spinner.stop_and_persist("😎", "Success").clear()
    return Result.Ok()
