from dataclasses import dataclass
from pathlib import Path

from lxml import html

from vigorish.util.numeric_helpers import ONE_KB


@dataclass
class UrlDetails:
    url: str
    identifier: str
    fileName: str
    htmlFolderPath: str
    s3KeyPrefix: str

    @property
    def local_file_path(self):
        return Path(self.htmlFolderPath).joinpath(self.fileName)

    @property
    def s3_object_key(self):
        if self.s3KeyPrefix[-1] != "/":
            self.s3KeyPrefix += "/"
        return f"{self.s3KeyPrefix}{self.fileName}"

    @property
    def file_exists_with_content(self):
        return self.local_file_path.exists() and self.local_file_path.stat().st_size > ONE_KB

    @property
    def page_content(self):
        if not self.file_exists_with_content:
            return None
        return html.fromstring(self.local_file_path.read_text(), base_url=self.url)

    def as_dict(self):
        return {
            "url": self.url,
            "identifier": self.identifier,
            "fileName": self.fileName,
            "htmlFolderPath": self.htmlFolderPath,
            "s3KeyPrefix": self.s3KeyPrefix,
        }
