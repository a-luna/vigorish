from datetime import datetime

import vigorish.database as db
from vigorish.enums import DataSet
from vigorish.scrape.bbref_boxscores.parse_html import parse_bbref_boxscore
from vigorish.scrape.bbref_games_for_date.parse_html import parse_bbref_dashboard_page
from vigorish.scrape.brooks_games_for_date.parse_html import parse_brooks_dashboard_page
from vigorish.scrape.brooks_pitch_logs.models.pitch_log import BrooksPitchLog
from vigorish.scrape.brooks_pitch_logs.models.pitch_logs_for_game import BrooksPitchLogsForGame
from vigorish.scrape.brooks_pitch_logs.parse_html import parse_pitch_log
from vigorish.scrape.brooks_pitchfx.parse_html import parse_pitchfx_log
from vigorish.status.update_status_bbref_boxscores import update_status_bbref_boxscore
from vigorish.status.update_status_bbref_games_for_date import (
    update_bbref_games_for_date_single_date as update_status_bbref_games_for_date,
)
from vigorish.status.update_status_brooks_games_for_date import (
    update_brooks_games_for_date_single_date as update_status_brooks_games_for_date,
)
from vigorish.status.update_status_brooks_pitch_logs import update_status_brooks_pitch_logs_for_game
from vigorish.status.update_status_brooks_pitchfx import update_pitch_appearance_status_records
from vigorish.tasks.combine_scraped_data import CombineScrapedDataTask

GAME_DATE_BR_DAILY = datetime(2018, 7, 26)
GAME_DATE_BB_DAILY = datetime(2018, 4, 17)
GAME_DATE_BBREF_BOX = datetime(2018, 3, 29)
GAME_DATE_PLOG = datetime(2018, 6, 17)
GAME_DATE_PFX = datetime(2018, 4, 1)

GAME_ID_BBREF_BOX = "ATL201803290"
GAME_ID_PLOG = "TOR201806170"
GAME_ID_PFX = "OAK201804010"

GAME_ID_NO_ERRORS = "TOR201906170"
NO_ERRORS_PITCH_APP = "TOR201906170_429719"
GAME_ID_WITH_ERRORS = "NYA201906112"
GAME_ID_NO_PFX_FOR_PITCH_APP = "PIT201909070"
GAME_ID_EXTRA_PFX_REMOVED = "TEX201904150"
GAME_ID_PFX_OUT_OF_SEQUENCE = "WAS201904030"
GAME_ID_PATCH_PFX = "OAK201904030"
GAME_ID_PATCH_BOXSCORE = "TOR201908170"

PATCH_BR_DAILY_GAME_DATE = datetime(2017, 9, 15)
PATCH_BB_DAILY_GAME_DATE = datetime(2017, 5, 26)
INVALID_BR_GAME_ID1 = "CHA201705261"
INVALID_BR_GAME_ID2 = "CHA201705262"
PATCHED_BR_GAME_ID1 = "CHA201705260"
PATCHED_BR_GAME_ID2 = "CHA201705272"

ALL_GD_GAME_DATE = datetime(2019, 7, 11)
ALL_GD_GAME_ID = "TEX201907110"

COMBINED_DATA_GAME_DICT = {
    "NO_ERRORS": {
        "bbref_game_id": GAME_ID_NO_ERRORS,
        "bb_game_id": "gid_2019_06_17_anamlb_tormlb_1",
        "game_date": datetime(2019, 6, 17),
        "apply_patch_list": False,
    },
    "WITH_ERRORS": {
        "bbref_game_id": GAME_ID_WITH_ERRORS,
        "bb_game_id": "gid_2019_06_11_nynmlb_nyamlb_2",
        "game_date": datetime(2019, 6, 11),
        "apply_patch_list": False,
    },
    "NO_PFX_FOR_PITCH_APP": {
        "bbref_game_id": GAME_ID_NO_PFX_FOR_PITCH_APP,
        "bb_game_id": "gid_2019_09_07_slnmlb_pitmlb_1",
        "game_date": datetime(2019, 9, 7),
        "apply_patch_list": False,
    },
    "EXTRA_PFX_REMOVED": {
        "bbref_game_id": GAME_ID_EXTRA_PFX_REMOVED,
        "bb_game_id": "gid_2019_04_15_anamlb_texmlb_1",
        "game_date": datetime(2019, 4, 15),
        "apply_patch_list": False,
    },
    "PATCH_PFX": {
        "bbref_game_id": GAME_ID_PATCH_PFX,
        "bb_game_id": "gid_2019_04_03_bosmlb_oakmlb_1",
        "game_date": datetime(2019, 4, 3),
        "apply_patch_list": True,
    },
    "PATCH_BOXSCORE": {
        "bbref_game_id": GAME_ID_PATCH_BOXSCORE,
        "bb_game_id": "gid_2019_08_17_seamlb_tormlb_1",
        "game_date": datetime(2019, 8, 17),
        "apply_patch_list": True,
    },
    "PFX_OUT_OF_SEQUENCE": {
        "bbref_game_id": GAME_ID_PFX_OUT_OF_SEQUENCE,
        "bb_game_id": "gid_2019_04_03_phimlb_wasmlb_1",
        "game_date": datetime(2019, 4, 3),
        "apply_patch_list": False,
    },
}


def seed_database_with_2019_test_data(vig_app):
    for game_id_dict in COMBINED_DATA_GAME_DICT.values():
        game_date = game_id_dict["game_date"]
        bbref_game_id = game_id_dict["bbref_game_id"]
        bb_game_id = game_id_dict["bb_game_id"]
        apply_patch_list = game_id_dict["apply_patch_list"]
        update_scraped_bbref_games_for_date(vig_app, game_date)
        update_scraped_brooks_games_for_date(vig_app, game_date)
        update_scraped_boxscore(vig_app, bbref_game_id)
        update_scraped_pitch_logs(vig_app, game_date, bbref_game_id)
        update_scraped_pitchfx_logs(vig_app, bb_game_id)
        result_dict = CombineScrapedDataTask(vig_app).execute(bbref_game_id, apply_patch_list)
        assert result_dict["gather_scraped_data_success"]
        assert result_dict["combined_data_success"]
        assert result_dict["update_pitch_apps_success"]
        vig_app.db_session.commit()


def update_scraped_bbref_games_for_date(vig_app, game_date):
    season = get_season(vig_app, game_date.year)
    bbref_games_for_date = parse_bbref_games_for_date_from_html(vig_app, game_date)
    result = update_status_bbref_games_for_date(vig_app.db_session, season, bbref_games_for_date)
    assert result.success
    return bbref_games_for_date


def parse_bbref_games_for_date_from_html(vig_app, game_date):
    bbref_url = get_bbref_url_for_date(game_date)
    html_path = vig_app.scraped_data.get_html(DataSet.BBREF_GAMES_FOR_DATE, game_date)
    result = parse_bbref_dashboard_page(html_path.read_text(), game_date, bbref_url)
    assert result.success
    bbref_games_for_date = result.value
    return bbref_games_for_date


def get_bbref_url_for_date(game_date):
    y = game_date.year
    m = game_date.month
    d = game_date.day
    return f"https://www.baseball-reference.com/boxes/?month={m}&day={d}&year={y}"


def update_scraped_brooks_games_for_date(vig_app, game_date, apply_patch_list=True):
    season = get_season(vig_app, game_date.year)
    brooks_games_for_date = parse_brooks_games_for_date_from_html(vig_app, game_date, apply_patch_list)
    result = update_status_brooks_games_for_date(vig_app.db_session, season, brooks_games_for_date)
    assert result.success
    return brooks_games_for_date


def get_season(vig_app, year):
    season = db.Season.find_by_year(vig_app.db_session, year)
    assert season
    return season


def parse_brooks_games_for_date_from_html(vig_app, game_date, apply_patch_list=True):
    html_path = vig_app.scraped_data.get_html(DataSet.BROOKS_GAMES_FOR_DATE, game_date)
    page_content = html_path.read_text()
    url = get_brooks_url_for_date(game_date)
    games_for_date = vig_app.scraped_data.get_bbref_games_for_date(game_date, apply_patch_list)
    result = parse_brooks_dashboard_page(vig_app.db_session, page_content, game_date, url, games_for_date)
    assert result.success
    brooks_games_for_date = result.value
    return brooks_games_for_date


def get_brooks_url_for_date(game_date):
    y = game_date.year
    m = game_date.month
    d = game_date.day
    return f"http://www.brooksbaseball.net/dashboard.php?dts={m}/{d}/{y}"


def update_scraped_boxscore(vig_app, bbref_game_id):
    bbref_boxscore = parse_bbref_boxscore_from_html(vig_app, bbref_game_id)
    result = update_status_bbref_boxscore(vig_app.db_session, bbref_boxscore)
    assert result.success
    return bbref_boxscore


def parse_bbref_boxscore_from_html(vig_app, bbref_game_id):
    url = get_bbref_boxscore_url(bbref_game_id)
    html_path = vig_app.scraped_data.get_html(DataSet.BBREF_BOXSCORES, bbref_game_id)
    result = parse_bbref_boxscore(html_path.read_text(), url, bbref_game_id)
    assert result.success
    bbref_boxscore = result.value
    return bbref_boxscore


def get_bbref_boxscore_url(bbref_game_id):
    team_id = bbref_game_id[:3]
    return f"https://www.baseball-reference.com/boxes/{team_id}/{bbref_game_id}.shtml"


def update_scraped_pitch_logs(vig_app, game_date, bbref_game_id, apply_patch_list=True):
    pitch_logs_for_game = parse_brooks_pitch_logs_for_game_from_html(
        vig_app, game_date, bbref_game_id, apply_patch_list
    )
    result = update_status_brooks_pitch_logs_for_game(vig_app.db_session, pitch_logs_for_game)
    assert result.success
    return pitch_logs_for_game


def parse_brooks_pitch_logs_for_game_from_html(vig_app, game_date, bbref_game_id, apply_patch_list=True):
    games_for_date = vig_app.scraped_data.get_brooks_games_for_date(game_date, apply_patch_list)
    game_info = [game for game in games_for_date.games if game.bbref_game_id == bbref_game_id][0]
    pitch_logs_for_game = BrooksPitchLogsForGame()
    pitch_logs_for_game.bb_game_id = game_info.bb_game_id
    pitch_logs_for_game.bbref_game_id = game_info.bbref_game_id
    pitch_logs_for_game.pitch_log_count = game_info.pitcher_appearance_count
    scraped_pitch_logs = []
    for pitcher_id, url in game_info.pitcher_appearance_dict.items():
        pitch_app_id = f"{bbref_game_id}_{pitcher_id}"
        html_path = vig_app.scraped_data.get_html(DataSet.BROOKS_PITCH_LOGS, pitch_app_id)
        result = parse_pitch_log(html_path.read_text(), game_info, pitcher_id, url)
        assert result.success
        pitch_log = result.value
        assert isinstance(pitch_log, BrooksPitchLog)
        scraped_pitch_logs.append(pitch_log)
    pitch_logs_for_game.pitch_logs = scraped_pitch_logs
    return pitch_logs_for_game


def revert_pitch_logs_to_state_before_combined_data(vig_app, bbref_game_id):
    result = vig_app.scraped_data.get_all_brooks_pitchfx_logs_for_game(bbref_game_id, False)
    assert result.success
    pfx_logs = result.value
    for pfx_log in pfx_logs:
        pitch_app_id = pfx_log.pitch_app_id
        pitch_app_status = db.PitchAppScrapeStatus.find_by_pitch_app_id(vig_app.db_session, pitch_app_id)
        assert pitch_app_status
        if not pitch_app_status.combined_pitchfx_bbref_data:
            continue
        pitch_app_status.combined_pitchfx_bbref_data = 0
        pitch_app_status.pitch_count_bbref = 0
        pitch_app_status.pitch_count_pitchfx_audited = 0
        pitch_app_status.extra_pitchfx_removed_count = 0
        pitch_app_status.missing_pitchfx_count = 0
        pitch_app_status.batters_faced_bbref = 0
        pitch_app_status.total_at_bats_pitchfx_complete = 0
        pitch_app_status.total_at_bats_missing_pitchfx = 0
    vig_app.db_session.commit()


def update_scraped_pitchfx_logs(vig_app, bb_game_id):
    pitchfx_logs_for_game = []
    pitch_logs_for_game = vig_app.scraped_data.get_brooks_pitch_logs_for_game(bb_game_id)
    for pitch_log in pitch_logs_for_game.pitch_logs:
        if not pitch_log.parsed_all_info:
            continue
        html_path = vig_app.scraped_data.get_html(DataSet.BROOKS_PITCHFX, pitch_log.pitch_app_id)
        result = parse_pitchfx_log(html_path.read_text(), pitch_log)
        assert result.success
        pfx_log = result.value
        pitchfx_logs_for_game.append(pfx_log)
        result = update_pitch_appearance_status_records(vig_app.db_session, pfx_log)
        assert result.success
    return pitchfx_logs_for_game


def combine_scraped_data_for_game(vig_app, game_id, apply_patch_list=True):
    result_dict = CombineScrapedDataTask(vig_app).execute(game_id, apply_patch_list, update_db=False)
    assert result_dict["combined_data_success"] and "boxscore" in result_dict
    return result_dict["boxscore"]
