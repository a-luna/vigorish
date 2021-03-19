from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.numeric_helpers import ONE_KB


@dataclass
class UrlDetails:
    url: str
    url_id: Union[str, datetime]
    fileName: str
    cachedHtmlFolderPath: str
    scrapedHtmlFolderpath: str

    @property
    def scraped_file_path(self):
        return Path(self.scrapedHtmlFolderpath).joinpath(self.fileName)

    @property
    def scraped_html_is_valid(self):
        return self.scraped_file_path.exists() and self.scraped_file_path.stat().st_size > ONE_KB

    @property
    def scraped_html(self):
        return self.scraped_file_path.read_text() if self.scraped_html_is_valid else None

    @property
    def cached_file_path(self):
        return Path(self.cachedHtmlFolderPath).joinpath(self.fileName)

    @property
    def cached_html_is_valid(self):
        return self.cached_file_path.exists() and self.cached_file_path.stat().st_size > ONE_KB

    @property
    def cached_html(self):
        return self.cached_file_path.read_text() if self.cached_html_is_valid else None

    @property
    def html_was_scraped(self):
        return self.cached_html_is_valid or self.scraped_html_is_valid

    @property
    def html(self):
        return self.cached_html or self.scraped_html or None

    def as_dict(self):
        valid_json_id = self.url_id.strftime(DATE_ONLY_2) if isinstance(self.url_id, datetime) else self.url_id
        return {
            "url": self.url,
            "url_id": valid_json_id,
            "htmlFileName": self.fileName,
            "cachedHtmlFolderPath": self.cachedHtmlFolderPath,
            "htmlFolderpath": self.scrapedHtmlFolderpath,
        }
