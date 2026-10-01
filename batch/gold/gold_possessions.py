from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

FPS = 25

@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.gold_possessions",
    comment="Possession sequences with team and opponent offside metrics, possession_id",
    partition_cols=["match_id"],
)
def gold_possession_zones():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.silver_positions_grouped")

    # Derive has_possession from ball_possession field via window (no subquery)
    w_frame = Window.partitionBy("match_id", "frame_id")
    all_team_frames = silver_df \
        .withColumn("_poss_team", F.max(F.when(F.col("team_id") == "BALL", F.expr("get(players, 0).ball_possession")).otherwise(F.lit(None))).over(w_frame)) \
        .filter(F.col("team_id") != "BALL") \
        .withColumn("has_possession", F.col("_poss_team") == F.col("team_id")) \
        .drop("_poss_team")

    all_team_frames = all_team_frames \
        .withColumn("_opp_offside_line",
            F.max(F.when(F.col("has_possession") == False, F.col("offside_line")).otherwise(F.lit(None))).over(w_frame)
        ) \
        .withColumn("_opp_team_id",
            F.max(F.when(F.col("has_possession") == False, F.col("team_id")).otherwise(F.lit(None))).over(w_frame)
        )

    possession_frames = all_team_frames.filter(F.col("has_possession") == True)

    possession_frames = possession_frames \
        .withColumn("row_num", F.row_number().over(Window.partitionBy("match_id", "game_section").orderBy("frame_id"))) \
        .withColumn("row_num_possession", F.row_number().over(Window.partitionBy("match_id", "game_section", "team_id").orderBy("frame_id"))) \
        .withColumn("group_id", F.col("row_num") - F.col("row_num_possession"))

    possession_sequences = possession_frames.groupBy("match_id", "game_section", "team_id", "group_id") \
        .agg(
            F.min("frame_id").alias("start_frame"),
            F.max("frame_id").alias("end_frame"),
            F.countDistinct("frame_id").alias("num_frames"),
            F.sum(F.when(F.col("play_state") == "active", 1).otherwise(0)).alias("active_frames"),
            F.sum(F.when(F.col("play_state") == "interruption", 1).otherwise(0)).alias("interruption_frames"),
            F.round(F.avg(F.when(F.expr("get(players, 0).distance") > 0, F.expr("get(players, 0).speed"))), 2).alias("avg_ball_speed"),
            F.round(F.avg("offside_line"), 2).alias("avg_offside_line"),
            F.round(F.avg("_opp_offside_line"), 2).alias("opponent_avg_offside_line"),
            F.max("_opp_team_id").alias("opponent_id"),
        ) \
        .withColumn("duration_sec", F.col("num_frames") / F.lit(FPS)) \
        .withColumn("duration_min", F.round(F.col("duration_sec") / 60, 2)) \
        .withColumn("cumulative_time", F.concat(
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) / 60).cast("int").cast("string"), 2, "0"),
            F.lit(":"),
            F.lpad(F.floor(F.sum(F.col("duration_sec")).over(Window.partitionBy("match_id", "game_section").orderBy("start_frame").rowsBetween(Window.unboundedPreceding, Window.currentRow)) % 60).cast("int").cast("string"), 2, "0")
        )) \
        .withColumn("possession_id", F.row_number().over(Window.partitionBy("match_id", "game_section").orderBy("start_frame"))) \
        .withColumn("team_metrics", F.struct(
            F.col("avg_ball_speed").alias("avg_ball_speed"),
            F.col("avg_offside_line").alias("avg_offside_line"),
        )) \
        .withColumn("opponent_metrics", F.struct(
            F.col("opponent_avg_offside_line").alias("avg_offside_line"),
        )) \
        .select("possession_id", "match_id", "game_section", "cumulative_time",
                "team_id", "opponent_id",
                "start_frame", "end_frame", "num_frames", "duration_sec", "duration_min",
                "active_frames", "interruption_frames",
                "team_metrics", "opponent_metrics")

    return possession_sequences