"""Database file and connection details."""
# flake8: noqa
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


from vigorish.models import (
    Assoc_Player_Team,
    BatStats,
    BatStatsCsvRow,
    DateScrapeStatus,
    DateScrapeStatusCsvRow,
    GameScrapeStatus,
    GameScrapeStatusCsvRow,
    PitchAppScrapeStatus,
    PitchAppScrapeStatusCsvRow,
    PitchFx,
    PitchFxCsvRow,
    PitchStats,
    PitchStatsCsvRow,
    Player,
    PlayerId,
    ScrapeError,
    ScrapeJob,
    Season,
    Team,
    TimeBetweenPitches,
)
from vigorish.views import (
    BatStats_All_View,
    BatStats_By_Opp_Team_View,
    BatStats_By_Opp_Team_Year_View,
    BatStats_By_Team_View,
    BatStats_By_Team_Year_View,
    BatStats_By_Year_View,
    Batter_PitchFx_All_View,
    Batter_PitchFx_By_Year_View,
    Batter_PitchFx_For_Game_All_View,
    Batter_PitchFx_For_Game_vs_LHP_as_LHB_View,
    Batter_PitchFx_For_Game_vs_LHP_as_RHB_View,
    Batter_PitchFx_For_Game_vs_RHP_as_LHB_View,
    Batter_PitchFx_For_Game_vs_RHP_as_RHB_View,
    Batter_PitchFx_vs_LHP_as_LHB_Career_View,
    Batter_PitchFx_vs_LHP_as_RHB_Career_View,
    Batter_PitchFx_vs_RHP_as_LHB_Career_View,
    Batter_PitchFx_vs_RHP_as_RHB_Career_View,
    Batter_PitchType_All_View,
    Batter_PitchType_By_Year_View,
    Batter_PitchType_For_Game_All_View,
    Batter_PitchType_For_Game_vs_LHP_as_LHB_View,
    Batter_PitchType_For_Game_vs_LHP_as_RHB_View,
    Batter_PitchType_For_Game_vs_RHP_as_LHB_View,
    Batter_PitchType_For_Game_vs_RHP_as_RHB_View,
    Batter_PitchType_vs_LHP_as_LHB_Career_View,
    Batter_PitchType_vs_LHP_as_RHB_Career_View,
    Batter_PitchType_vs_RHP_as_LHB_Career_View,
    Batter_PitchType_vs_RHP_as_RHB_Career_View,
    Date_PitchApp_View,
    Game_PitchApp_View,
    Pitcher_PitchFx_All_By_Year_View,
    Pitcher_PitchFx_All_View,
    Pitcher_PitchFx_For_Game_All_View,
    Pitcher_PitchFx_For_Game_vs_LHB_View,
    Pitcher_PitchFx_For_Game_vs_RHB_View,
    Pitcher_PitchFx_vs_LHB_By_Year_View,
    Pitcher_PitchFx_vs_LHB_View,
    Pitcher_PitchFx_vs_RHB_By_Year_View,
    Pitcher_PitchFx_vs_RHB_View,
    Pitcher_PitchType_All_By_Year_View,
    Pitcher_PitchType_All_View,
    Pitcher_PitchType_For_Game_All_View,
    Pitcher_PitchType_For_Game_vs_LHB_View,
    Pitcher_PitchType_For_Game_vs_RHB_View,
    Pitcher_PitchType_vs_LHB_By_Year_View,
    Pitcher_PitchType_vs_LHB_View,
    Pitcher_PitchType_vs_RHB_By_Year_View,
    Pitcher_PitchType_vs_RHB_View,
    PitchStats_All_View,
    PitchStats_By_Opp_Team_By_Year_View,
    PitchStats_By_Opp_Team_View,
    PitchStats_By_Team_By_Year_View,
    PitchStats_By_Team_View,
    PitchStats_By_Year_View,
    PitchStats_RP_View,
    PitchStats_SP_View,
    PitchType_All_View,
    PitchType_Left_View,
    PitchType_Right_View,
    Season_Date_View,
    Season_Game_PitchApp_View,
    Season_Game_View,
    Season_PitchApp_View,
    Team_BatStats_By_BatOrder_By_Player_By_Year,
    Team_BatStats_By_BatOrder_By_Year,
    Team_BatStats_By_DefPosition_By_Player_By_Year,
    Team_BatStats_By_DefPosition_By_Year,
    Team_BatStats_By_Player_By_Year_View,
    Team_BatStats_By_Year_View,
    Team_BatStats_For_Starters_By_Player_By_Year,
    Team_BatStats_For_Starters_By_Year,
    Team_BatStats_For_Subs_By_Player_By_Year,
    Team_BatStats_For_Subs_By_Year,
    Team_PitchStats_By_Player_By_Year_View,
    Team_PitchStats_By_Year_View,
    Team_PitchStats_RP_By_Player_By_Year_View,
    Team_PitchStats_RP_By_Year_View,
    Team_PitchStats_SP_By_Player_By_Year_View,
    Team_PitchStats_SP_By_Year_View,
)
