from datetime import datetime

from vigorish.util.list_helpers import as_dict_list


class BBRefPlayerTransaction:
    """Information (e.g., teams, players, dates) related to a MLB transaction such as a trade."""

    category_id = 0
    date = datetime(1000, 1, 1)
    description = ""
    details = []

    def as_dict(self):
        return dict(
            category_id=self.category_id,
            date=self.date.strftime("%Y-%m-%d"),
            description=self.description,
            details=as_dict_list(self.details),
        )
