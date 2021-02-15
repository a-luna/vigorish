from dacite import from_dict

from vigorish.enums import DataSet, VigFile
from vigorish.scrape.url_details import UrlDetails
from vigorish.util.datetime_util import get_date_range
from vigorish.util.dt_format_strings import DATE_MONTH_NAME
from vigorish.util.result import Result


def create_url_set(start_date, end_date, db_job, data_set, scraped_data):
    url_set = {}
    for game_date in get_date_range(start_date, end_date):
        result = create_url_set_for_date(db_job, data_set, scraped_data, game_date)
        if result.failure:
            return result
        url_set[game_date] = result.value
    return Result.Ok(url_set)


def create_url_set_for_date(db_job, data_set, scraped_data, game_date):
    create_url_set_dict = {
        DataSet.BROOKS_GAMES_FOR_DATE: create_url_for_brooks_games_for_date,
        DataSet.BROOKS_PITCH_LOGS: create_urls_for_brooks_pitch_logs_for_date,
        DataSet.BROOKS_PITCHFX: create_urls_for_brooks_pitchfx_logs_for_date,
        DataSet.BBREF_GAMES_FOR_DATE: create_url_for_bbref_games_for_date,
        DataSet.BBREF_BOXSCORES: create_urls_for_bbref_boxscores_for_date,
    }
    return create_url_set_dict[data_set](db_job, scraped_data, game_date)


def create_url_for_brooks_games_for_date(db_job, scraped_data, game_date):
    data_set = DataSet.BROOKS_GAMES_FOR_DATE
    url_data = {
        "url": get_url_for_brooks_games_for_date(game_date),
        "url_id": game_date,
        "fileName": get_filename(scraped_data, data_set, game_date),
        "cachedHtmlFolderPath": get_cached_html_folderpath(scraped_data, data_set, game_date),
        "scrapedHtmlFolderpath": get_scraped_html_folderpath(db_job, data_set),
    }
    return Result.Ok([from_dict(data_class=UrlDetails, data=url_data)])


def create_urls_for_brooks_pitch_logs_for_date(db_job, scraped_data, game_date):
    data_set = DataSet.BROOKS_PITCH_LOGS
    req_data_set = DataSet.BROOKS_GAMES_FOR_DATE
    games_for_date = scraped_data.get_brooks_games_for_date(game_date)
    if not games_for_date:
        return Result.Fail(get_unscraped_data_error(data_set, req_data_set, game_date))
    urls = []
    for game in games_for_date.games:
        if game.might_be_postponed:
            continue
        for pitcher_id, pitch_log_url in game.pitcher_appearance_dict.items():
            pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
            url_data = {
                "url": pitch_log_url,
                "url_id": pitch_app_id,
                "fileName": get_filename(scraped_data, data_set, pitch_app_id),
                "cachedHtmlFolderPath": get_cached_html_folderpath(scraped_data, data_set, game_date),
                "scrapedHtmlFolderpath": get_scraped_html_folderpath(db_job, data_set),
            }
            urls.append(from_dict(data_class=UrlDetails, data=url_data))
    return Result.Ok(urls)


def create_urls_for_brooks_pitchfx_logs_for_date(db_job, scraped_data, game_date):
    data_set = DataSet.BROOKS_PITCHFX
    req_data_set_1 = DataSet.BROOKS_GAMES_FOR_DATE
    req_data_set_2 = DataSet.BROOKS_PITCH_LOGS
    games_for_date = scraped_data.get_brooks_games_for_date(game_date)
    if not games_for_date:
        return Result.Fail(get_unscraped_data_error(data_set, req_data_set_1, game_date))
    urls = []
    if not games_for_date.game_count:
        return Result.Ok(urls)
    pitch_logs_for_date = scraped_data.get_all_brooks_pitch_logs_for_date(game_date)
    if not pitch_logs_for_date:
        return Result.Fail(get_unscraped_data_error(data_set, req_data_set_2, game_date))
    for pitch_logs_for_game in pitch_logs_for_date:
        for pitch_log in pitch_logs_for_game.pitch_logs:
            if not pitch_log.parsed_all_info:
                continue
            pitch_app_id = f"{pitch_log.bbref_game_id}_{pitch_log.pitcher_id_mlb}"
            url_data = {
                "url": pitch_log.pitchfx_url,
                "url_id": pitch_app_id,
                "fileName": get_filename(scraped_data, data_set, pitch_app_id),
                "cachedHtmlFolderPath": get_cached_html_folderpath(scraped_data, data_set, game_date),
                "scrapedHtmlFolderpath": get_scraped_html_folderpath(db_job, data_set),
            }
            urls.append(from_dict(data_class=UrlDetails, data=url_data))
    return Result.Ok(urls)


def create_url_for_bbref_games_for_date(db_job, scraped_data, game_date):
    data_set = DataSet.BBREF_GAMES_FOR_DATE
    url_data = {
        "url": get_url_for_bbref_games_for_date(game_date),
        "url_id": game_date,
        "fileName": get_filename(scraped_data, data_set, game_date),
        "cachedHtmlFolderPath": get_cached_html_folderpath(scraped_data, data_set, game_date),
        "scrapedHtmlFolderpath": get_scraped_html_folderpath(db_job, data_set),
    }
    return Result.Ok([from_dict(data_class=UrlDetails, data=url_data)])


def create_urls_for_bbref_boxscores_for_date(db_job, scraped_data, game_date):
    data_set = DataSet.BBREF_BOXSCORES
    req_data_set = DataSet.BBREF_GAMES_FOR_DATE
    bbref_games_for_date = scraped_data.get_bbref_games_for_date(game_date)
    if not bbref_games_for_date:
        return Result.Fail(get_unscraped_data_error(data_set, req_data_set, game_date))
    urls = []
    for game_info in bbref_games_for_date.games:
        url_data = {
            "url": game_info.url,
            "url_id": game_info.bbref_game_id,
            "fileName": get_filename(scraped_data, data_set, game_info.bbref_game_id),
            "cachedHtmlFolderPath": get_cached_html_folderpath(scraped_data, data_set, game_date),
            "scrapedHtmlFolderpath": get_scraped_html_folderpath(db_job, data_set),
        }
        urls.append(from_dict(data_class=UrlDetails, data=url_data))
    return Result.Ok(urls)


def get_unscraped_data_error(data_set, req_data_set, game_date):
    return (
        f"Unable to create {data_set} URLs for {game_date.strftime(DATE_MONTH_NAME)} since "
        f"{req_data_set} for this date has not been scraped."
    )


def get_url_for_brooks_games_for_date(game_date):
    month = game_date.month
    day = game_date.day
    year = game_date.year
    return f"https://www.brooksbaseball.net/dashboard.php?dts={month}/{day}/{year}"


def get_url_for_bbref_games_for_date(game_date):
    month = game_date.month
    day = game_date.day
    year = game_date.year
    return f"https://www.baseball-reference.com/boxes/?month={month}&day={day}&year={year}"


def get_filename(scraped_data, data_set, identifier):
    return scraped_data.file_helper.filename_dict[VigFile.SCRAPED_HTML][data_set](identifier)


def get_cached_html_folderpath(scraped_data, data_set, game_date):
    return scraped_data.file_helper.get_local_folderpath(VigFile.SCRAPED_HTML, data_set, game_date)


def get_scraped_html_folderpath(db_job, data_set):
    folderpath = db_job.scraped_html_folders[data_set]
    return str(folderpath.resolve())
