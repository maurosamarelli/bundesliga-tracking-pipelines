from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.silver_positions_grouped",
    comment="Grouped tracking data with players struct, offside line, play state, possession flag",
    partition_cols=["match_id"],
)
def silver_positions():
    silver_df = spark.read.table("`bundesliga-2022-2023`.batch.silver_positions_enrichment")

    # Filter out referee rows
    silver_df = silver_df.filter(F.col("team_id") != "referee")
    player_cols = [
        "person_id", "speed", "distance", "acceleration",
        "x_norm", "y_norm",
        "ball_possession", "ball_status",
        "ball_distance", "has_possession",
        "pair_player_id", "pair_player_distance",
        "closest_opponent_distance", "target_distance",
    ]
    silver_grouped = silver_df.groupBy(
        "match_id", "team_id", "frame_id", "game_section",
    ).agg(
        F.collect_list(F.struct(*[F.col(c) for c in player_cols])).alias("players"),
    ).withColumn(
        "players",
        F.expr("array_sort(players, (a, b) -> CASE WHEN a.x_norm < b.x_norm THEN -1 WHEN a.x_norm > b.x_norm THEN 1 ELSE 0 END)"),
    )
    silver_grouped = silver_grouped.withColumn(
        "offside_line", F.expr("get(players, 1).x_norm"),
    )

    # Add play_state (window on ball_status)
    w = Window.partitionBy("match_id", "frame_id")
    silver_grouped = silver_grouped.withColumn(
        "play_state",
        F.when(
            F.max(F.when(F.col("team_id") == "BALL", F.expr("get(players, 0).ball_status"))).over(w) == 1,
            "active",
        ).otherwise("interruption"),
    )

    return silver_grouped
