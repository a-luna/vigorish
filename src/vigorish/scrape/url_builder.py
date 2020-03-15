from dataclasses import dataclass
from datetime import datetime
from typing import List

from vigorish.enums import DataSet, DocFormat
from vigorish.data.scraped_data import ScrapedData
from vigorish.util.datetime_util import get_date_range
from vigorish.util.string_helpers import (
    validate_bbref_game_id,
    validate_brooks_game_id,
    validate_pitch_app_id,
)


@dataclass
class ScrapeUrl:
    url: str
    data_set: DataSet
    game_date: datetime
    bbref_game_id: str
    pitch_app_id: str


class UrlBuilder:
    def __init__(self, config, file_helper):
        self.config = config
        self.file_helper = file_helper

    @property
    def create_url_set_dict(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.create_urls_for_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.create_urls_for_brooks_pitch_logs_for_date,
            DataSet.BROOKS_PITCHFX: self.create_urls_for_brooks_pitchfx_logs_for_date,
            DataSet.BBREF_GAMES_FOR_DATE: self.create_urls_for_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.create_urls_for_bbref_boxscores_for_date,
        }

    @property
    def filename_dict(self):
        html_filename_dict = self.file_helper.filename_dict[DocFormat.HTML]
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: html_filename_dict[DataSet.BROOKS_GAMES_FOR_DATE],
            DataSet.BROOKS_PITCH_LOGS: html_filename_dict[DataSet.BROOKS_PITCH_LOGS],
            DataSet.BROOKS_PITCHFX: html_filename_dict[DataSet.BROOKS_PITCHFX],
            DataSet.BBREF_GAMES_FOR_DATE: html_filename_dict[DataSet.BBREF_GAMES_FOR_DATE],
            DataSet.BBREF_BOXSCORES: html_filename_dict[DataSet.BBREF_BOXSCORES],
        }

    @property
    def url_dict(self):
        return {
            DataSet.BROOKS_GAMES_FOR_DATE: self.get_url_for_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.get_url_for_brooks_pitch_log,
            DataSet.BROOKS_PITCHFX: self.get_url_for_brooks_pitchfx_log,
            DataSet.BBREF_GAMES_FOR_DATE: self.get_url_for_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.get_url_for_bbref_boxscore,
        }

    def create_url_set(
        data_set: DataSet, start_date: datetime, end_date: datetime
    ) -> List[ScrapeUrl]:
        url_set = []
        for game_date in get_date_range(start_date, end_date):
            url_set.extend(self.create_url_set_dict[data_set](game_date))

    def create_urls_for_brooks_games_for_date(self, game_date: datetime) -> List[ScrapeUrl]:
        return ScrapeUrl(url=self.get_url_for_brooks_games_for_date(game_date))

    def create_urls_for_brooks_pitch_logs_for_date(self, game_date: datetime) -> List[ScrapeUrl]:
        pass

    def create_urls_for_brooks_pitch_logs_for_game(
        self, game_date: datetime, bb_game_id: str
    ) -> List[ScrapeUrl]:
        pass

    def create_urls_for_brooks_pitchfx_logs_for_date(self, game_date: datetime) -> List[ScrapeUrl]:
        pass

    def create_urls_for_brooks_pitchfx_logs_for_game(
        self, game_date: datetime, bb_game_id: str
    ) -> List[ScrapeUrl]:
        pass

    def create_urls_for_bbref_games_for_date(self, game_date: datetime) -> List[ScrapeUrl]:
        pass

    def create_urls_for_bbref_boxscores_for_date(self, game_date: datetime) -> List[ScrapeUrl]:
        pass

    def get_url_for_brooks_games_for_date(self, game_date: datetime) -> str:
        month = game_date.month
        day = game_date.day
        year = game_date.year
        return f"http://www.brooksbaseball.net/dashboard.php?dts={month}/{day}/{year}"

    def get_url_for_brooks_pitch_log(self, bb_game_id: str, pitch_app_id: str) -> str:
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        year = game_dict["game_date"].year
        month = game_dict["game_date"].month
        day = game_dict["game_date"].day
        pitcher_id = game_dict["pitcher_id"]
        return f"http://brooksbaseball.net/pfx/index.php?s_type=3&sp_type=1&batterX=0&year={year}&month={month}&day={day}&pitchSel={pitcher_id}.xml&game={bb_game_id}/"

    def get_url_for_brooks_pitchfx_log(self, bb_game_id: str, pitch_app_id: str) -> str:
        result = validate_pitch_app_id(pitch_app_id)
        if result.failure:
            return result
        game_dict = result.value
        pitcher_id = game_dict["pitcher_id"]
        return f"http://www.brooksbaseball.net/pfxVB/tabdel_expanded.php?pitchSel={pitcher_id}&game={bb_game_id}/&s_type=3&h_size=700&v_size=500"

    def get_url_for_bbref_games_for_date(self, game_date: datetime) -> str:
        month = game_date.month
        day = game_date.day
        year = game_date.year
        return f"https://www.baseball-reference.com/boxes/?month={month}&day={day}&year={year}"

    def get_url_for_bbref_boxscore(self, bbref_game_id: str) -> str:
        team_id = bbref_game_id[:3]
        return f"https://www.baseball-reference.com/boxes/{team_id}/{bbref_game_id}.shtml"
