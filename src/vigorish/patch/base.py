import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from typing import List

from vigorish.enums import DataSet
from vigorish.util.list_helpers import as_dict_list
from vigorish.util.result import Result


@dataclass
class Patch(ABC):
    patch_id: str = field(repr=False, init=False)
    data_set: DataSet = field(repr=False, init=False)

    def as_dict(self):
        patch_dict = {self.patch_id: True}
        patch_dict.update(asdict(self))
        patch_dict.pop("patch_id")
        patch_dict.pop("data_set")
        return patch_dict

    def as_json(self):
        return json.dumps(self.as_dict(), indent=2)

    @abstractmethod
    def __post_init__(self):
        pass

    @abstractmethod
    def apply(self, data):
        pass


@dataclass
class PatchList(ABC):
    patch_list_id: str = field(repr=False, init=False)
    data_set: DataSet = field(repr=False, init=False)
    patch_list: List[Patch] = field(repr=False)
    url_id: str

    def as_dict(self):
        return {
            self.patch_list_id: True,
            "url_id": self.url_id,
            "patch_list": as_dict_list(self.patch_list),
        }

    def as_json(self):
        return json.dumps(self.as_dict(), indent=2)

    def apply(self, data, db_session, boxscore=None):
        data = self.pre_process_data(data)
        for patch in self.patch_list:
            result = patch.apply(data)
            if result.failure:
                return result
            data = result.value
        data = self.post_process_data(data, db_session, boxscore)
        return Result.Ok(data)

    @abstractmethod
    def __post_init__(self):
        pass

    def pre_process_data(self, data):
        return data

    def post_process_data(self, data, db_session, boxscore=None):
        return data