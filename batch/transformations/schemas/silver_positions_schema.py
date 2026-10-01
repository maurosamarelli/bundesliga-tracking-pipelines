from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, IntegerType,
)

SILVER_POSITIONS_SCHEMA = StructType([
    StructField("match_id", StringType(), True),
    StructField("game_section", StringType(), True),
    StructField("frame_id", LongType(), True),
    StructField("team_id", StringType(), True),
    StructField("person_id", StringType(), True),
    StructField("x_norm", DoubleType(), True),
    StructField("y_norm", DoubleType(), True),
    StructField("distance", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("acceleration", DoubleType(), True),
    StructField("ball_possession", StringType(), True),
    StructField("ball_status", IntegerType(), True),
    StructField("ball_distance", DoubleType(), True),
    StructField("has_possession", BooleanType(), True),
    StructField("pair_player_id", StringType(), True),
    StructField("pair_player_distance", DoubleType(), True),
    StructField("closest_opponent_distance", DoubleType(), True),
    StructField("target_distance", DoubleType(), True),
])

SILVER_OUTPUT_COLUMNS = [
    "match_id", "game_section", "frame_id", "team_id", "person_id",
    "x_norm", "y_norm", "distance", "speed", "acceleration",
    "ball_possession", "ball_status",
    "ball_distance", "has_possession",
    "pair_player_id", "pair_player_distance",
    "closest_opponent_distance", "target_distance",
]
