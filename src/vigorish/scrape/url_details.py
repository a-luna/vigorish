from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Union

from vigorish.util.dt_format_strings import DATE_ONLY_2
from vigorish.util.numeric_helpers import ONE_KB


@dataclass
class UrlDetails:
    url: str
    identifier: Union[str, datetime]
    fileName: str
    htmlFolderPath: str
    scrapedHtmlFolderpath: str

    @property
    def scraped_file_path(self):
        return Path(self.scrapedHtmlFolderpath).joinpath(self.fileName)

    @property
    def scraped_file_exists_with_content(self):
        return self.scraped_file_path.exists() and self.scraped_file_path.stat().st_size > ONE_KB

    @property
    def scraped_page_content(self):
        if not self.scraped_file_exists_with_content:
            return None
        return self.scraped_file_path.read_text()

    @property
    def local_file_path(self):
        return Path(self.htmlFolderPath).joinpath(self.fileName)

    @property
    def local_file_exists_with_content(self):
        return self.local_file_path.exists() and self.local_file_path.stat().st_size > ONE_KB

    @property
    def local_page_content(self):
        if not self.local_file_exists_with_content:
            return None
        return self.local_file_path.read_text()

    @property
    def file_exists_with_content(self):
        return self.local_file_exists_with_content or self.scraped_file_exists_with_content

    @property
    def html(self):
        return (
            self.local_page_content
            if self.local_file_exists_with_content
            else self.scraped_page_content
            if self.scraped_file_exists_with_content
            else None
        )

    def move_scraped_file_to_local_folder(self):
        self.scraped_file_path.replace(self.local_file_path)

    def as_dict(self):
        valid_json_id = (
            self.identifier.strftime(DATE_ONLY_2)
            if isinstance(self.identifier, datetime)
            else self.identifier
        )
        return {
            "url": self.url,
            "identifier": valid_json_id,
            "fileName": self.fileName,
            "htmlFolderPath": self.htmlFolderPath,
            "scrapedHtmlFolderpath": self.scrapedHtmlFolderpath,
        }
