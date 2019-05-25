from halo import Halo

from app.main.models.season import Season
from app.main.status.update_status_bbref_boxscores import update_data_set_bbref_boxscores
from app.main.status.update_status_bbref_games_for_date import update_data_set_bbref_games_for_date
from app.main.status.update_status_brooks_games_for_date import update_data_set_brooks_games_for_date
from app.main.status.update_status_brooks_pitch_logs import update_data_set_brooks_pitch_logs
from app.main.util.result import Result


def update_status_for_mlb_season(session, year):
    spinner = Halo(text=f'Updating MLB {year}...', color='yellow', spinner='dots3')
    spinner.start()

    season = Season.find_by_year(session, year)
    if not season:
        spinner.fail("Fail")
        error = f'Error occurred retrieving season for year={year}'
        return Result.Fail(error)

    spinner.text = f'Updating MLB {year} bbref_games_for_date...'
    result = update_data_set_bbref_games_for_date(session, season)
    if result.failure:
        spinner.fail("Fail")
        return result

    spinner.text = f'Updating MLB {year} brooks_games_for_date...'
    spinner.color = "green"
    result = update_data_set_brooks_games_for_date(session, season)
    if result.failure:
        spinner.fail("Fail")
        return result

    spinner.text = f'Updating MLB {year} bbref_boxscores...'
    spinner.color = "cyan"
    result = update_data_set_bbref_boxscores(session, season)
    if result.failure:
        spinner.fail("Fail")
        return result

    spinner.text = f'Updating MLB {year} brooks_pitch_logs...'
    spinner.color = "magenta"
    result = update_data_set_brooks_pitch_logs(session, season)
    if result.failure:
        spinner.fail("Fail")
        return result

    spinner.text = ""
    spinner.clear()
    session.commit()
    return Result.Ok()
