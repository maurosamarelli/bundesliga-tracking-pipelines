from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="`bundesliga-2022-2023`.batch.raw_positions",
    comment="Raw tracking data from JSONL files — one row per entity per frame, cast to proper types",
    partition_cols=["match_id"], #partition pruning
)
def raw_positions():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://bundesliga-2022-2023-data/timemajor/")
        .select(
            F.col("n").alias("frame_id"),
            F.explode("frames").alias("frame_obj"),
        )
        .select(
            F.col("frame_obj.FrameSet.MatchId").alias("match_id"),
            F.col("frame_obj.FrameSet.GameSection").alias("game_section"),
            F.col("frame_id"),
            F.col("frame_obj.FrameSet.TeamId").alias("team_id"),
            F.col("frame_obj.FrameSet.PersonId").alias("person_id"),
            F.to_timestamp(F.col("frame_obj.Frame.T")).alias("timestamp"),
            F.col("frame_obj.Frame.X").cast("double").alias("x"),
            F.col("frame_obj.Frame.Y").cast("double").alias("y"),
            F.col("frame_obj.Frame.Z").cast("double").alias("z"),
            F.col("frame_obj.Frame.D").cast("double").alias("distance"),
            F.col("frame_obj.Frame.S").cast("double").alias("speed"),
            F.col("frame_obj.Frame.A").cast("double").alias("acceleration"),
            F.col("frame_obj.Frame.M").cast("int").alias("m_flag"),
            F.col("frame_obj.Frame.BallPossession").cast("int").alias("ball_possession"),
            F.col("frame_obj.Frame.BallStatus").cast("int").alias("ball_status"),
        )
    )