from datetime import datetime
from typing import List

from dacite import from_dict

from vigorish.enums import DataSet, DocFormat
from vigorish.scrape.url_details import UrlDetails
from vigorish.util.regex import BBREF_BOXSCORE_URL_REGEX
from vigorish.util.result import Result


class UrlBuilder:
    def __init__(self, scrape_job, config, scraped_data):
        self.scrape_job = scrape_job
        self.config = config
        self.scraped_data = scraped_data

    def create_url_set(self, data_set: DataSet) -> Result:
        url_set = {}
        for game_date in self.scrape_job.date_range:
            result = self.create_url_set_for_date(data_set, game_date)
            if result.failure:
                return result
            url_set[game_date] = result.value
        return Result.Ok(url_set)

    def create_url_set_for_date(self, data_set: DataSet, game_date: datetime):
        create_url_set_dict = {
            DataSet.BROOKS_GAMES_FOR_DATE: self.create_url_for_brooks_games_for_date,
            DataSet.BROOKS_PITCH_LOGS: self.create_urls_for_brooks_pitch_logs_for_date,
            DataSet.BROOKS_PITCHFX: self.create_urls_for_brooks_pitchfx_logs_for_date,
            DataSet.BBREF_GAMES_FOR_DATE: self.create_url_for_bbref_games_for_date,
            DataSet.BBREF_BOXSCORES: self.create_urls_for_bbref_boxscores_for_date,
        }
        return create_url_set_dict[data_set](game_date)

    def create_url_for_brooks_games_for_date(self, game_date: datetime) -> List[dict]:
        data_set = DataSet.BROOKS_GAMES_FOR_DATE
        url_data = {
            "identifier": game_date,
            "fileName": self.get_filename(data_set, game_date),
            "htmlFolderPath": self.get_local_folderpath(data_set, game_date),
            "scrapedHtmlFolderpath": self.get_scraped_html_folderpath(data_set),
            "s3KeyPrefix": self.get_s3_folderpath(data_set, game_date),
            "url": self.get_url_for_brooks_games_for_date(game_date),
        }
        return Result.Ok([from_dict(data_class=UrlDetails, data=url_data)])

    def create_urls_for_brooks_pitch_logs_for_date(self, game_date: datetime) -> List[dict]:
        data_set = DataSet.BROOKS_PITCH_LOGS
        result = self.scraped_data.get_brooks_games_for_date(game_date)
        if result.failure:
            return result
        games_for_date = result.value
        urls = []
        for game in games_for_date.games:
            if game.might_be_postponed:
                continue
            for pitcher_id, pitch_log_url in game.pitcher_appearance_dict.items():
                pitch_app_id = f"{game.bbref_game_id}_{pitcher_id}"
                url_data = {
                    "identifier": pitch_app_id,
                    "fileName": self.get_filename(data_set, pitch_app_id),
                    "htmlFolderPath": self.get_local_folderpath(data_set, game_date),
                    "scrapedHtmlFolderpath": self.get_scraped_html_folderpath(data_set),
                    "s3KeyPrefix": self.get_s3_folderpath(data_set, game_date),
                    "url": pitch_log_url,
                }
                urls.append(from_dict(data_class=UrlDetails, data=url_data))
        return Result.Ok(urls)

    def create_urls_for_brooks_pitchfx_logs_for_date(self, game_date: datetime) -> List[dict]:
        data_set = DataSet.BROOKS_PITCHFX
        result = self.scraped_data.get_all_brooks_pitch_logs_for_date(game_date)
        if result.failure:
            return result
        pitch_logs_for_date = result.value
        urls = []
        for pitch_logs_for_game in pitch_logs_for_date:
            for pitch_log in pitch_logs_for_game.pitch_logs:
                if not pitch_log.parsed_all_info:
                    continue
                pitch_app_id = f"{pitch_log.bbref_game_id}_{pitch_log.pitcher_id_mlb}"
                url_data = {
                    "identifier": pitch_app_id,
                    "fileName": self.get_filename(data_set, pitch_app_id),
                    "htmlFolderPath": self.get_local_folderpath(data_set, game_date),
                    "scrapedHtmlFolderpath": self.get_scraped_html_folderpath(data_set),
                    "s3KeyPrefix": self.get_s3_folderpath(data_set, game_date),
                    "url": pitch_log.pitchfx_url,
                }
                urls.append(from_dict(data_class=UrlDetails, data=url_data))
        return Result.Ok(urls)

    def create_url_for_bbref_games_for_date(self, game_date: datetime) -> List[dict]:
        data_set = DataSet.BBREF_GAMES_FOR_DATE
        url_data = {
            "identifier": game_date,
            "fileName": self.get_filename(data_set, game_date),
            "htmlFolderPath": self.get_local_folderpath(data_set, game_date),
            "scrapedHtmlFolderpath": self.get_scraped_html_folderpath(data_set),
            "s3KeyPrefix": self.get_s3_folderpath(data_set, game_date),
            "url": self.get_url_for_bbref_games_for_date(game_date),
        }
        return Result.Ok([from_dict(data_class=UrlDetails, data=url_data)])

    def create_urls_for_bbref_boxscores_for_date(self, game_date: datetime) -> List[dict]:
        data_set = DataSet.BBREF_BOXSCORES
        result = self.scraped_data.get_bbref_games_for_date(game_date)
        if result.failure:
            return result
        games_for_date = result.value
        urls = []
        for boxscore_url in games_for_date.boxscore_urls:
            bbref_game_id = self.get_bbref_game_id_from_url(boxscore_url)
            url_data = {
                "identifier": bbref_game_id,
                "fileName": self.get_filename(data_set, bbref_game_id),
                "htmlFolderPath": self.get_local_folderpath(data_set, game_date),
                "scrapedHtmlFolderpath": self.get_scraped_html_folderpath(data_set),
                "s3KeyPrefix": self.get_s3_folderpath(data_set, game_date),
                "url": boxscore_url,
            }
            urls.append(from_dict(data_class=UrlDetails, data=url_data))
        return Result.Ok(urls)

    def get_url_for_brooks_games_for_date(self, game_date: datetime) -> str:
        month = game_date.month
        day = game_date.day
        year = game_date.year
        return f"http://www.brooksbaseball.net/dashboard.php?dts={month}/{day}/{year}"

    def get_url_for_bbref_games_for_date(self, game_date: datetime) -> str:
        month = game_date.month
        day = game_date.day
        year = game_date.year
        return f"https://www.baseball-reference.com/boxes/?month={month}&day={day}&year={year}"

    def get_filename(self, data_set: DataSet, identifier: str) -> str:
        return self.scraped_data.file_helper.filename_dict[DocFormat.HTML][data_set](identifier)

    def get_local_folderpath(self, data_set: DataSet, game_date: datetime) -> str:
        return self.scraped_data.file_helper.get_local_folderpath(
            doc_format=DocFormat.HTML, data_set=data_set, game_date=game_date
        )

    def get_scraped_html_folderpath(self, data_set: DataSet) -> str:
        folderpath = self.scrape_job.get_scraped_html_folderpath(data_set)
        return str(folderpath.resolve())

    def get_s3_folderpath(self, data_set: DataSet, game_date: datetime) -> str:
        return self.scraped_data.file_helper.get_s3_folderpath(
            doc_format=DocFormat.HTML, data_set=data_set, game_date=game_date
        )

    def get_bbref_game_id_from_url(self, url: str) -> str:
        match = BBREF_BOXSCORE_URL_REGEX.search(url)
        if not match:
            return Result.Fail(f"Failed to parse bbref_game_id from url: {url}")
        id_dict = match.groupdict()
        return id_dict["game_id"]
