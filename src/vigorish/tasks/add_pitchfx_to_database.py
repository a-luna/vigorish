from events import Events

from vigorish.config.database import PitchFx, PitchAppScrapeStatus
from vigorish.data.all_game_data import AllGameData
from vigorish.tasks.base import Task
from vigorish.util.result import Result


class AddPitchFxToDatabase(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "add_pitchfx_to_db_start",
                "add_pitchfx_to_db_progress",
                "add_pitchfx_to_db_complete",
            )
        )

    def execute(self, audit_report, year):
        report_for_season = audit_report.get(year)
        if not report_for_season:
            return Result.Fail(f"Audit report could not be generated for MLB Season {year}")
        game_ids = report_for_season.get("successful")
        if not game_ids:
            error = f"No games for MLB Season {year} qualify to have PitchFx data imported."
            return Result.Fail(error)
        self.events.add_pitchfx_to_db_start(game_ids)
        for num, game_id in enumerate(game_ids, start=1):
            self.add_pitchfx_to_database(game_id)
            self.events.add_pitchfx_to_db_progress(num, game_id)
        self.events.add_pitchfx_to_db_complete()
        return Result.Ok()

    def add_pitchfx_to_database(self, game_id):
        all_game_data = AllGameData(self.db_session, self.scraped_data, game_id)
        for pfx_dict in all_game_data.get_all_pitchfx():
            pfx = PitchFx.from_dict(pfx_dict)
            pfx.update_relationships(self.db_session)
            self.db_session.add(pfx)
        for pitch_app_id in all_game_data.get_all_pitch_app_ids():
            pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
            pitch_app.imported_pitchfx = 1
        self.db_session.commit()
