"""
JSON Schema definitions for pipeline transformations.
Schemas are pre-defined based on actual source data structure.

To update these schemas:
1. Sample the source data: spark.read.format("delta").load(path).select(col("value").cast("string"))
2. Use: SELECT schema_of_json(value) FROM source LIMIT 1
3. Update the DDL string below with the result
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    _parse_datatype_string,
)

# Spark 4.x's _parse_datatype_string requires an active SparkSession, so a
# private context may be needed at import time. Crucially, that context must
# NOT become the process-wide active session: otherwise every consumer's later
# SparkSession.builder...getOrCreate() reuses THIS session and silently ignores
# the consumer's master()/config() settings (seen as the pairs consumer
# running under the "bundesliga-2022-2023-schemas" app). Only spin up a
# session when none exists yet, and stop() it again before import returns so
# consumers always build their own context with their own configuration.
_SESSION_EXISTED_BEFORE = SparkSession.getActiveSession()

if _SESSION_EXISTED_BEFORE is None:
    _PARSE_CTX = (
        SparkSession.builder
        .appName("bundesliga-2022-2023-schemas")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# Schema DDL strings - inferred from actual data

POSITIONS_SCHEMA_DDL = """STRUCT<Frame: STRUCT<A: STRING, D: STRING, M: STRING, N: STRING, S: STRING, T: STRING, X: STRING, Y: STRING, BallPossession: STRING, BallStatus: STRING>,FrameSet: STRUCT<GameSection: STRING, MatchId: STRING, PersonId: STRING, TeamId: STRING>>"""

POSITIONS_PROJECTION_SCHEMA_DDL = """STRUCT<Frame: STRUCT<A: STRING, D: STRING, N: STRING, S: STRING, X: STRING, Y: STRING, BallPossession: STRING, BallStatus: STRING>,FrameSet: STRUCT<GameSection: STRING, MatchId: STRING, PersonId: STRING, TeamId: STRING>>"""

# Events schema - comprehensive structure including ALL possible EventDetails fields
# Note: EventDetails structure varies by EventType - most fields will be null for any given event
# - When EventType="Play", play fields are directly in EventDetails (Player, Team, etc.)
# - When EventType="FreeKick"/"GoalKick"/etc., play fields are in EventDetails.Play
EVENTS_SCHEMA_DDL = """STRUCT<EventDetails: STRUCT<AfterFreeKick: STRING, AmountOfDefenders: STRING, AngleToGoal: STRING, AssistAction: STRING, AssistShotAtGoal: STRING, AssistTypeShotAtGoal: STRING, BallPossessionPhase: STRING, BuildUp: STRING, ChanceEvaluation: STRING, CounterAttack: STRING, Cross: STRING, DecisionTimestamp: STRING, DefensiveClearance: STRING, Distance: STRING, DistanceToGoal: STRING, DribbleEvaluation: STRING, DribblingSide: STRING, DribblingType: STRING, Evaluation: STRING, ExecutionMode: STRING, ExtendedTypeOfShot: STRING, FlatCross: STRING, FoulType: STRING, Fouled: STRING, Fouler: STRING, FromOpenPlay: STRING, GameSection: STRING, GoalDistanceGoalkeeper: STRING, GoalKeeperAction: STRING, GoalKeeperInvolved: STRING, Height: STRING, InsideBox: STRING, Loser: STRING, LoserRole: STRING, LoserTeam: STRING, Pass: STRUCT<Direction: STRING, FreeKickLayup: STRING>, PenaltyBox: STRING, Play: STRUCT<BallPossessionPhase: STRING, Distance: STRING, Evaluation: STRING, FlatCross: STRING, FromOpenPlay: STRING, Height: STRING, Pass: STRUCT<Direction: STRING, FreeKickLayup: STRING>, PenaltyBox: STRING, PlayAngle: STRING, PlayOrigin: STRING, Player: STRING, Recipient: STRING, SemiField: STRING, Team: STRING>, PlayAngle: STRING, PlayOrigin: STRING, Player: STRING, PlayerSpeed: STRING, PossessionChange: STRING, Pressure: STRING, Recipient: STRING, Rotation: STRING, SemiField: STRING, SetupOrigin: STRING, ShotCondition: STRING, ShotOrigin: STRING, ShotWide: STRING, Side: STRING, SignificanceEvaluation: STRING, SuccessfulShot: STRING, TakerBallControl: STRING, TakerSetup: STRING, Team: STRING, TeamFouled: STRING, TeamFouler: STRING, TeamLeft: STRING, TeamRight: STRING, Type: STRING, TypeOfShot: STRING, Winner: STRING, WinnerAction: STRING, WinnerResult: STRING, WinnerRole: STRING, WinnerTeam: STRING, xG: STRING>, EventId: STRING, EventTime: STRING, EventType: STRING, MatchId: STRING, `X-Position`: STRING, `X-Source-Position`: STRING, `Y-Position`: STRING, `Y-Source-Position`: STRING>"""

MATCHINFORMATION_SCHEMA_DDL = """STRUCT<Environment: STRUCT<AirHumidity: STRING, AirPressure: STRING, Country: STRING, Floodlight: STRING, NeutralVenue: STRING, NumberOfSpectators: STRING, PitchErosion: STRING, PitchX: STRING, PitchY: STRING, Precipitation: STRING, Roof: STRING, SoldOut: STRING, StadiumAddress: STRING, StadiumCapacity: STRING, StadiumId: STRING, StadiumName: STRING, Temperature: STRING>, General: STRUCT<CompetitionId: STRING, CompetitionName: STRING, DlProviderId: STRING, GuestTeamId: STRING, GuestTeamName: STRING, HomeTeamId: STRING, HomeTeamName: STRING, Host: STRING, KickoffTime: STRING, MatchDay: STRING, MatchId: STRING, MatchTitle: STRING, PlannedKickoffTime: STRING, Result: STRING, Season: STRING, SeasonId: STRING, Type: STRING, TypeOfSport: STRING>, OtherGameInformation: STRUCT<PlayingTimeFirstHalf: STRING, PlayingTimeSecondHalf: STRING, TotalTimeFirstHalf: STRING, TotalTimeSecondHalf: STRING>, Referees: ARRAY<STRUCT<FirstName: STRING, LastName: STRING, PersonId: STRING, Role: STRING, Shortname: STRING>>, Teams: ARRAY<STRUCT<LineUp: STRING, PlayerShirtMainColor: STRING, PlayerShirtNumberColor: STRING, PlayerShirtSecondaryColor: STRING, PlayerShirtType: STRING, Players: ARRAY<STRUCT<FirstName: STRING, LastName: STRING, PersonId: STRING, PlayingPosition: STRING, ShirtNumber: STRING, Shortname: STRING, Starting: STRING, TeamLeader: STRING>>, Role: STRING, TeamId: STRING, TeamName: STRING, TrainerStaff: ARRAY<STRUCT<FirstName: STRING, LastName: STRING, PersonId: STRING, Role: STRING, Shortname: STRING>>>>>"""

# Parse DDL strings to StructType
POSITIONS_SCHEMA = _parse_datatype_string(POSITIONS_SCHEMA_DDL)
POSITIONS_PROJECTION_SCHEMA = _parse_datatype_string(POSITIONS_PROJECTION_SCHEMA_DDL)
EVENTS_SCHEMA = _parse_datatype_string(EVENTS_SCHEMA_DDL)
MATCHINFORMATION_SCHEMA = _parse_datatype_string(MATCHINFORMATION_SCHEMA_DDL)

SILVER_POSITIONS_SCHEMA = StructType([
    StructField("source_timestamp", LongType(), True),
    StructField("batch_id", LongType(), True),
    StructField("match_id", StringType(), True),
    StructField("game_section", StringType(), True),
    StructField("frame_id", LongType(), True),
    StructField("team_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("X", DoubleType(), True),
    StructField("Y", DoubleType(), True),
    StructField("D", DoubleType(), True),
    StructField("S", DoubleType(), True),
    StructField("A", DoubleType(), True),
    StructField("ball_possession", IntegerType(), True),
    StructField("ball_status", IntegerType(), True),
    StructField("ball_distance", DoubleType(), True),
    StructField("has_possession", BooleanType(), True),
    StructField("pair_player_id", StringType(), True),
    StructField("pair_player_distance", DoubleType(), True),
    StructField("closest_opponent_distance", DoubleType(), True),
    StructField("target_distance", DoubleType(), True),
])

# Release the throwaway context (only the one created above) so consumers
# are guaranteed a clean slate for their own builder/getOrCreate().
if _SESSION_EXISTED_BEFORE is None:
    _PARSE_CTX.stop()

# Helper functions for parsing and flattening

def flatten_event_details(parsed_col_name="event"):
    """
    Create a flattened EventDetails struct from a parsed event column.
    Merges nested Play fields with top-level EventDetails fields.
    
    Usage:
        .select(
            from_json(col("value").cast("string"), EVENTS_SCHEMA).alias("event")
        )
        .select(
            "*",
            col("event.*"),
            flatten_event_details("event")
        )
    
    Args:
        parsed_col_name: Name of the column containing the parsed JSON (default: "event")
    
    Returns:
        Struct column with flattened EventDetails
    """
    from pyspark.sql.functions import col, coalesce, struct
    
    p = f"{parsed_col_name}.EventDetails"
    
    return struct(
        # Fields that may be at top-level OR in Play - use COALESCE to merge
        coalesce(col(f"{p}.BallPossessionPhase"), col(f"{p}.Play.BallPossessionPhase")).alias("BallPossessionPhase"),
        coalesce(col(f"{p}.Distance"), col(f"{p}.Play.Distance")).alias("Distance"),
        coalesce(col(f"{p}.Evaluation"), col(f"{p}.Play.Evaluation")).alias("Evaluation"),
        coalesce(col(f"{p}.FlatCross"), col(f"{p}.Play.FlatCross")).alias("FlatCross"),
        coalesce(col(f"{p}.FromOpenPlay"), col(f"{p}.Play.FromOpenPlay")).alias("FromOpenPlay"),
        coalesce(col(f"{p}.Height"), col(f"{p}.Play.Height")).alias("Height"),
        coalesce(col(f"{p}.PenaltyBox"), col(f"{p}.Play.PenaltyBox")).alias("PenaltyBox"),
        coalesce(col(f"{p}.PlayAngle"), col(f"{p}.Play.PlayAngle")).alias("PlayAngle"),
        coalesce(col(f"{p}.PlayOrigin"), col(f"{p}.Play.PlayOrigin")).alias("PlayOrigin"),
        coalesce(col(f"{p}.Player"), col(f"{p}.Play.Player")).alias("Player"),
        coalesce(col(f"{p}.Recipient"), col(f"{p}.Play.Recipient")).alias("Recipient"),
        coalesce(col(f"{p}.SemiField"), col(f"{p}.Play.SemiField")).alias("SemiField"),
        coalesce(col(f"{p}.Team"), col(f"{p}.Play.Team")).alias("Team"),
        # Pass fields - merge top-level and nested
        coalesce(col(f"{p}.Pass.Direction"), col(f"{p}.Play.Pass.Direction")).alias("PassDirection"),
        coalesce(col(f"{p}.Pass.FreeKickLayup"), col(f"{p}.Play.Pass.FreeKickLayup")).alias("PassFreeKickLayup"),
        # All other EventDetails fields (only at top-level, not in Play)
        col(f"{p}.AfterFreeKick").alias("AfterFreeKick"),
        col(f"{p}.AmountOfDefenders").alias("AmountOfDefenders"),
        col(f"{p}.AngleToGoal").alias("AngleToGoal"),
        col(f"{p}.AssistAction").alias("AssistAction"),
        col(f"{p}.AssistShotAtGoal").alias("AssistShotAtGoal"),
        col(f"{p}.AssistTypeShotAtGoal").alias("AssistTypeShotAtGoal"),
        col(f"{p}.BuildUp").alias("BuildUp"),
        col(f"{p}.ChanceEvaluation").alias("ChanceEvaluation"),
        col(f"{p}.CounterAttack").alias("CounterAttack"),
        col(f"{p}.Cross").alias("Cross"),
        col(f"{p}.DecisionTimestamp").alias("DecisionTimestamp"),
        col(f"{p}.DefensiveClearance").alias("DefensiveClearance"),
        col(f"{p}.DistanceToGoal").alias("DistanceToGoal"),
        col(f"{p}.DribbleEvaluation").alias("DribbleEvaluation"),
        col(f"{p}.DribblingSide").alias("DribblingSide"),
        col(f"{p}.DribblingType").alias("DribblingType"),
        col(f"{p}.ExecutionMode").alias("ExecutionMode"),
        col(f"{p}.ExtendedTypeOfShot").alias("ExtendedTypeOfShot"),
        col(f"{p}.FoulType").alias("FoulType"),
        col(f"{p}.Fouled").alias("Fouled"),
        col(f"{p}.Fouler").alias("Fouler"),
        col(f"{p}.GameSection").alias("GameSection"),
        col(f"{p}.GoalDistanceGoalkeeper").alias("GoalDistanceGoalkeeper"),
        col(f"{p}.GoalKeeperAction").alias("GoalKeeperAction"),
        col(f"{p}.GoalKeeperInvolved").alias("GoalKeeperInvolved"),
        col(f"{p}.InsideBox").alias("InsideBox"),
        col(f"{p}.Loser").alias("Loser"),
        col(f"{p}.LoserRole").alias("LoserRole"),
        col(f"{p}.LoserTeam").alias("LoserTeam"),
        col(f"{p}.PlayerSpeed").alias("PlayerSpeed"),
        col(f"{p}.PossessionChange").alias("PossessionChange"),
        col(f"{p}.Pressure").alias("Pressure"),
        col(f"{p}.Rotation").alias("Rotation"),
        col(f"{p}.SetupOrigin").alias("SetupOrigin"),
        col(f"{p}.ShotCondition").alias("ShotCondition"),
        col(f"{p}.ShotOrigin").alias("ShotOrigin"),
        col(f"{p}.ShotWide").alias("ShotWide"),
        col(f"{p}.Side").alias("Side"),
        col(f"{p}.SignificanceEvaluation").alias("SignificanceEvaluation"),
        col(f"{p}.SuccessfulShot").alias("SuccessfulShot"),
        col(f"{p}.TakerBallControl").alias("TakerBallControl"),
        col(f"{p}.TakerSetup").alias("TakerSetup"),
        col(f"{p}.TeamFouled").alias("TeamFouled"),
        col(f"{p}.TeamFouler").alias("TeamFouler"),
        col(f"{p}.TeamLeft").alias("TeamLeft"),
        col(f"{p}.TeamRight").alias("TeamRight"),
        col(f"{p}.Type").alias("Type"),
        col(f"{p}.TypeOfShot").alias("TypeOfShot"),
        col(f"{p}.Winner").alias("Winner"),
        col(f"{p}.WinnerAction").alias("WinnerAction"),
        col(f"{p}.WinnerResult").alias("WinnerResult"),
        col(f"{p}.WinnerRole").alias("WinnerRole"),
        col(f"{p}.WinnerTeam").alias("WinnerTeam"),
        col(f"{p}.xG").alias("xG")
    ).alias("EventDetails")
