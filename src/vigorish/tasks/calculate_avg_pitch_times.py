"""Calculate average time between pitches during at bat, between at bats and between innings."""
from datetime import datetime, timezone

from events import Events

from vigorish.config.database import Season_Game_PitchApp_View as Season_View
from vigorish.tasks.base import Task
from vigorish.util.datetime_util import TIME_ZONE_NEW_YORK
from vigorish.util.numeric_helpers import trim_data_set
from vigorish.util.regex import PFX_TIMESTAMP_REGEX
from vigorish.util.result import Result
from vigorish.util.string_helpers import validate_bbref_game_id


class CalculateAverageTimeBetweenPitches(Task):
    def __init__(self, app):
        super().__init__(app)
        self.events = Events(
            (
                "find_eligible_games_start",
                "find_eligible_games_complete",
                "calculate_pitch_metrics_start",
                "calculate_pitch_metrics_progress",
                "calculate_pitch_metrics_complete",
            )
        )

    def execute(self, trim_data_sets=True):
        self.events.find_eligible_games_start()
        game_ids = Season_View.get_all_bbref_game_ids_combined_no_missing_pfx(self.db_engine)
        if not game_ids:
            return Result.Fail("No games meet the requirements for this process.")
        self.events.find_eligible_games_complete(game_ids)
        self.events.calculate_pitch_metrics_start()
        pitch_samples = []
        at_bat_samples = []
        inning_samples = []
        for num, game_id in enumerate(game_ids, start=1):
            combined_data = self.scraped_data.get_combined_game_data(game_id)
            if not combined_data:
                continue
            result = self.calc_pitch_metrics(combined_data)
            pitch_samples.extend(result[0])
            at_bat_samples.extend(result[2])
            inning_samples.extend(result[4])
            self.events.calculate_pitch_metrics_progress(num)
        self.events.calculate_pitch_metrics_complete()
        metrics = {
            "time_between_pitches": self.process_data_set(
                pitch_samples, trim=trim_data_sets, st_dev=0.2
            ),
            "time_between_at_bats": self.process_data_set(
                at_bat_samples, trim=trim_data_sets, st_dev=0.25
            ),
            "time_between_innings": self.process_data_set(
                inning_samples, trim=trim_data_sets, st_dev=0.007
            ),
        }
        return Result.Ok(metrics)

    def calc_pitch_metrics(self, combined_data):
        pitch_samples = []
        at_bat_samples = []
        inning_samples = []
        prev_ab_pfx = None
        prev_inn_pfx = None
        for half_inning in combined_data["play_by_play_data"]:
            for ab_num, at_bat in enumerate(half_inning["inning_events"], start=1):
                if (
                    at_bat["at_bat_pitchfx_audit"]["pitchfx_error"]
                    or at_bat["at_bat_pitchfx_audit"]["missing_pitchfx_count"]
                ):
                    continue
                for count, pfx in enumerate(at_bat["pitchfx"], start=1):
                    if count == 1 and prev_inn_pfx and ab_num == 1:
                        between_innings = self.get_seconds_between_pitches(prev_inn_pfx, pfx)
                        if between_innings > 0:
                            inning_samples.append(between_innings)
                        prev_inn_pfx = None
                    if count == 1 and prev_ab_pfx and ab_num != 1:
                        between_at_bats = self.get_seconds_between_pitches(prev_ab_pfx, pfx)
                        if between_at_bats > 0:
                            at_bat_samples.append(between_at_bats)
                        prev_ab_pfx = None
                    if count == len(at_bat["pitchfx"]):
                        if ab_num == len(half_inning["inning_events"]):
                            prev_inn_pfx = pfx
                        else:
                            prev_ab_pfx = pfx
                        continue
                    between_pitches = self.get_seconds_between_pitches(
                        pfx, at_bat["pitchfx"][count]
                    )
                    if between_pitches > 0:
                        pitch_samples.append(between_pitches)
        return (
            pitch_samples,
            self.process_data_set(pitch_samples),
            at_bat_samples,
            self.process_data_set(at_bat_samples),
            inning_samples,
            self.process_data_set(inning_samples),
        )

    def get_seconds_between_pitches(self, pitch1, pitch2):
        pitch1_thrown = self.get_time_pitch_thrown(pitch1)
        pitch2_thrown = self.get_time_pitch_thrown(pitch2)
        if not pitch1_thrown or not pitch2_thrown:
            return 0
        return (pitch2_thrown - pitch1_thrown).total_seconds()

    def get_time_pitch_thrown(self, pfx):
        match = PFX_TIMESTAMP_REGEX.match(pfx["park_sv_id"])
        if not match:
            return None
        group_dict = match.groupdict()
        game_dict = validate_bbref_game_id(pfx["bbref_game_id"]).value
        try:
            timestamp = datetime(
                game_dict["game_date"].year,
                int(group_dict["month"]),
                int(group_dict["day"]),
                int(group_dict["hour"]),
                int(group_dict["minute"]),
                int(group_dict["second"]),
            )
        except ValueError:
            return None
        return timestamp.replace(tzinfo=timezone.utc).astimezone(TIME_ZONE_NEW_YORK)

    def process_data_set(self, data_set, trim=False, st_dev=1):
        if not data_set or not isinstance(data_set, list):
            return {"error": "Data set is empty or is not a valid list"}
        if trim:
            data_set = trim_data_set(data_set, st_dev_limit=st_dev)
        if not data_set:
            return {"error": f"Trim process with st_dev_limit={st_dev} eliminated all data"}
        results = {
            "total": int(sum(data_set)),
            "count": int(len(data_set)),
            "avg": round(sum(data_set) / len(data_set), 1),
            "max": int(max(data_set)),
            "min": int(min(data_set)),
            "range": int(max(data_set) - min(data_set)),
            "trim": trim,
        }
        if trim:
            results["trim_stdev"] = st_dev
        return results
