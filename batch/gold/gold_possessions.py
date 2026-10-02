from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FPS = 25

@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.gold_possessions",
    comment="Possession sequences with team and opponent offside metrics, possession_id",
    partition_cols=["match_id"],
)
def gold_possessions():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.silver_positions_enrichment")
    _sort = "array_sort(_raw, (a, b) -> CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END)"
    all_frames = (
        silver_df
        .filter(F.col("ball_status") == 1)
        .groupBy("match_id", "game_section", "frame_id")
        .agg(
            F.max("ball_possession").alias("team_id"),
            F.max(F.when(F.col("team_id") == "BALL", F.col("speed"))).alias("ball_speed"),
            F.max(F.when((F.col("team_id") != "BALL") & (F.col("ball_possession") != F.col("team_id")), F.col("team_id"))).alias("opponent_id"),
            F.collect_list(F.when((F.col("team_id") != "BALL") & (F.col("ball_possession") == F.col("team_id")), F.col("x_norm"))).alias("_team_raw"),
            F.collect_list(F.when((F.col("team_id") != "BALL") & (F.col("ball_possession") != F.col("team_id")), F.col("x_norm"))).alias("_opp_raw"),
        )
        .withColumn("offside_line", F.expr(f"get({_sort.replace('_raw', '_team_raw')}, 1)"))
        .withColumn("_opp_offside_line", F.expr(f"get({_sort.replace('_raw', '_opp_raw')}, 1)"))
        .drop("_team_raw", "_opp_raw")
    )

    possession_frames = all_frames \
        .withColumn("row_num_possession", F.row_number().over(Window.partitionBy("match_id", "game_section", "team_id").orderBy("frame_id"))) \
        .withColumn("group_id", F.col("frame_id") - F.col("row_num_possession"))

    possession_sequences = possession_frames.groupBy("match_id", "game_section", "team_id", "group_id") \
        .agg(
            F.max("opponent_id").alias("opponent_id"),
            F.min("frame_id").alias("possession_id"),
            F.round(F.countDistinct("frame_id") / F.lit(FPS), 2).alias("duration_sec"),
            F.round(F.avg("ball_speed"), 2).alias("avg_ball_speed"),
            F.round(F.avg("offside_line"), 2).alias("avg_offside_line"),
            F.round(F.avg("_opp_offside_line"), 2).alias("opponent_avg_offside_line"),
        ) \
        .drop("group_id")

    return possession_sequences