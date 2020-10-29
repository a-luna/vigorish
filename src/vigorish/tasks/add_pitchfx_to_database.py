from events import Events

from vigorish.config.database import PitchAppScrapeStatus, PitchFx
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

    def execute(self, audit_report, year=None):
        self.audit_report = audit_report
        return self.add_pfx_for_year(year) if year else self.add_all_pfx()

    def add_all_pfx(self):
        valid_years = [year for year, results in self.audit_report.items() if results["successful"]]
        for year in valid_years:
            self.add_pfx_for_year(year)
        return Result.Ok()

    def add_pfx_for_year(self, year):
        report_for_season = self.audit_report.get(year)
        if not report_for_season:
            return Result.Fail(f"Audit report could not be generated for MLB Season {year}")
        game_ids = report_for_season.get("successful")
        if not game_ids:
            error = f"No games for MLB Season {year} qualify to have PitchFx data imported."
            return Result.Fail(error)
        self.events.add_pitchfx_to_db_start(year, game_ids)
        for num, game_id in enumerate(game_ids, start=1):
            self.add_pitchfx_to_database(game_id)
            self.events.add_pitchfx_to_db_progress(num, year, game_id)
        self.events.add_pitchfx_to_db_complete(year)
        return Result.Ok()

    def add_pitchfx_to_database(self, game_id):
        all_game_data = AllGameData(self.db_session, self.scraped_data, game_id)
        for pitch_app_id, pfx_dict_list in all_game_data.get_all_pitchfx().items():
            pitch_app = PitchAppScrapeStatus.find_by_pitch_app_id(self.db_session, pitch_app_id)
            if not pitch_app:
                error = f"PitchFx import aborted! Pitch app '{pitch_app_id}' not found in database"
                return Result.Fail(error)
            if pitch_app.imported_pitchfx:
                continue
            for pfx_dict in pfx_dict_list:
                pfx = PitchFx.from_dict(pfx_dict)
                pfx.update_relationships(self.db_session)
                self.db_session.add(pfx)
            pitch_app.imported_pitchfx = 1
        self.db_session.commit()
