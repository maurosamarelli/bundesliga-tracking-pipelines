from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    name="`bundesliga-2022-2023`.batch.match_info",
    comment="Match metadata from XML files — one row per match"
)
def match_info():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("rowTag", "MatchInformation")
        .option("encoding", "UTF-8")
        .load("s3://bundesliga-2022-2023-data/matchinformation/")
        .select(
            F.col("General._MatchId").alias("match_id"),
            F.col("Environment._PitchX").alias("pitch_x"),
            F.col("Environment._PitchY").alias("pitch_y"),
            F.col("General._HomeTeamId").alias("home_team_id"),
            F.col("General._GuestTeamId").alias("guest_team_id"),
        )
    )