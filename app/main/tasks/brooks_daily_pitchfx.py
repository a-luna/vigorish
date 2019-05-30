from app.main.models.status_date import DateScrapeStatus
from app.main.tasks.base_task import BaseTask
from app.main.util.s3_helper import get_all_brooks_pitch_logs_for_date_from_s3


class ScrapeBrooksDailyPitchFx(BaseTask):
    key_name = "brooks_pitchfx"
    display_name = "PitchFX for pitching appearance (brooksbaseball.com)"
