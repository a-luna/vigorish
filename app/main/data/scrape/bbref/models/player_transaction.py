from datetime import datetime


class BbrefPlayerTransaction():
    """Detailed information describing teams, players and dates related to a MLB transaction such as a trade."""

    category_id = 0
    date = datetime(1000, 1, 1)
    description = ""
    details = []

    def as_dict(self):
        dict = {
            "category_id": self.category_id,
            "date": "{}".format(self.date.strftime("%Y-%m-%d")),
            "description": "{}".format(self.description),
            "details": self._flatten(self.details)
        }
        return dict

    @staticmethod
    def _flatten(objects):
        return [obj.as_dict() for obj in objects]