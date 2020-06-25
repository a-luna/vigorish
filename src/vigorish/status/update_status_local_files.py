from halo import Halo

from vigorish.cli.util import get_random_cli_color
from vigorish.enums import DataSet, DocFormat
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore_list
from vigorish.status.update_status_bbref_games_for_date import update_bbref_games_for_date_list
from vigorish.status.update_status_brooks_games_for_date import (
    update_status_brooks_games_for_date_list,
)
from vigorish.status.update_status_brooks_pitch_logs import (
    update_status_brooks_pitch_logs_for_game_list,
)
from vigorish.status.update_status_brooks_pitchfx import update_status_brooks_pitchfx_log_list
from vigorish.util.result import Result


def local_folder_has_parsed_data_for_season(scraped_data, year):
    for data_set in DataSet:
        if data_set == DataSet.ALL:
            continue
        scraped_ids = scraped_data.get_scraped_ids_from_local_folder(
            DocFormat.JSON, data_set, year
        )
        if scraped_ids:
            return True
    return False


def update_all_data_sets(scraped_data, db_session, year, overwrite_existing=False):
    for data_set in DataSet:
        if data_set == DataSet.ALL:
            continue
        result = update_data_set(data_set, scraped_data, db_session, year, overwrite_existing)
        if result.failure:
            return result
    return Result.Ok()


def update_data_set(data_set, scraped_data, db_session, year, overwrite_existing=False):
    scraped_ids = scraped_data.get_scraped_ids_from_local_folder(DocFormat.JSON, data_set, year)
    if not scraped_ids:
        return Result.Ok()
    spinner = Halo(spinner="dots3", color=get_random_cli_color())
    spinner.text = f"Updating {data_set} for MLB {year}..."
    spinner.start()
    if overwrite_existing:
        result = update_status_for_data_set(data_set, scraped_data, db_session, scraped_ids)
        if result.failure:
            spinner.fail(f"Error occurred while updating {data_set} for MLB {year}")
            return result
        spinner.succeed(f"Successfully updated {data_set} for MLB {year}!")
        return Result.Ok()
    existing_scraped_ids = get_scraped_ids_from_database(scraped_data, data_set, year)
    new_stats_ids = list(set(scraped_ids) - set(existing_scraped_ids))
    result = update_status_for_data_set(data_set, scraped_data, db_session, new_stats_ids)
    if result.failure:
        spinner.fail(f"Error occurred while updating {data_set} for MLB {year}")
        return result
    db_session.commit()
    spinner.succeed(f"Successfully updated {data_set} for MLB {year}!")
    return Result.Ok()


def update_status_for_data_set(data_set, scraped_data, db_session, scraped_ids):
    update_status_dict = {
        DataSet.BROOKS_GAMES_FOR_DATE: update_status_brooks_games_for_date_list,
        DataSet.BROOKS_PITCH_LOGS: update_status_brooks_pitch_logs_for_game_list,
        DataSet.BROOKS_PITCHFX: update_status_brooks_pitchfx_log_list,
        DataSet.BBREF_GAMES_FOR_DATE: update_bbref_games_for_date_list,
        DataSet.BBREF_BOXSCORES: update_status_bbref_boxscore_list,
    }
    return update_status_dict[data_set](scraped_data, db_session, scraped_ids)


def get_scraped_ids_from_database(scraped_data, data_set, year):
    get_scraped_ids_from_db_dict = {
        DataSet.BROOKS_GAMES_FOR_DATE: scraped_data.get_all_brooks_dates_scraped,
        DataSet.BROOKS_PITCH_LOGS: scraped_data.get_all_scraped_brooks_game_ids,
        DataSet.BROOKS_PITCHFX: scraped_data.get_all_scraped_pitchfx_pitch_app_ids,
        DataSet.BBREF_GAMES_FOR_DATE: scraped_data.get_all_bbref_dates_scraped,
        DataSet.BBREF_BOXSCORES: scraped_data.get_all_scraped_bbref_game_ids,
    }
    return get_scraped_ids_from_db_dict[data_set](year)
