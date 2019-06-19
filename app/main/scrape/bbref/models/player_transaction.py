from datetime import datetime

from app.main.util.list_functions import as_dict_list


class BBRefPlayerTransaction():
    """Detailed information describing teams, players and dates related to a MLB transaction such as a trade."""

    category_id = 0
    date = datetime(1000, 1, 1)
    description = ""
    details = []

    def as_dict(self):
        return dict(
            category_id=self.category_id,
            date=self.date.strftime("%Y-%m-%d"),
            description=self.description,
            details=as_dict_list(self.details))
