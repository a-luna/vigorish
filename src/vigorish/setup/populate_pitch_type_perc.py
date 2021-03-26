import json

from tqdm import tqdm

from vigorish.database import PitchTypePercentile
from vigorish.enums import PitchType
from vigorish.util.result import Result

JSON_FILE_MAP = {
    "thrown_r": "pitch_type_percentiles_rhp.json",
    "thrown_l": "pitch_type_percentiles_lhp.json",
    "thrown_both": "pitch_type_percentiles_both.json",
}


def populate_pitch_type_perc(app, json_folder):
    with tqdm(
        total=len(JSON_FILE_MAP),
        desc="Populating percentiles table...",
        unit="file",
        mininterval=0.12,
        maxinterval=5,
        unit_scale=True,
        ncols=90,
    ) as pbar:
        for pitch_thrown, filename in JSON_FILE_MAP.items():
            json_file = json_folder.joinpath(filename)
            for stat_name, stat_map in json.loads(json_file.read_text()).items():
                for pitch_type_abbrev, pitch_type_map in stat_map.items():
                    pitch_type = PitchType.from_abbrev(pitch_type_abbrev)
                    if PitchType.PERCENTILES & pitch_type == pitch_type:
                        for percentile, stat_value in pitch_type_map.items():
                            percentile_dict = {
                                "stat_name": stat_name,
                                "pitch_type": pitch_type_abbrev,
                                "thrown_r": pitch_thrown == "thrown_r",
                                "thrown_l": pitch_thrown == "thrown_l",
                                "thrown_both": pitch_thrown == "thrown_both",
                                "percentile": percentile,
                                "stat_value": stat_value,
                            }
                            app.db_session.add(PitchTypePercentile(**percentile_dict))
            pbar.update()
    app.db_session.commit()
    return Result.Ok()
