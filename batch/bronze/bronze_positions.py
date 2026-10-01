from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.bronze_positions",
    comment="Enriched tracking data with attacking direction, normalized coordinates",
    partition_cols=["match_id"], #partition pruning
)
def bronze_positions():
    positions = spark.read.table("`bundesliga-2022-2023`.batch.raw_positions")

    # Attack direction map as inline scalar subquery (no join, no temp view)
    ad_map = F.expr("""
        (SELECT map_from_entries(collect_list(struct(
            struct(match_id, game_section, team_id),
            CASE WHEN abs(min_x) > abs(max_x) THEN 1 ELSE -1 END
        )))
        FROM (
            SELECT match_id, game_section, team_id, min(x) as min_x, max(x) as max_x
            FROM `bundesliga-2022-2023`.batch.raw_positions
            WHERE frame_id IN (10000, 100000) AND team_id NOT IN ('BALL', 'referee')
            GROUP BY match_id, game_section, team_id
        ))
    """)
    bronze_df = positions.withColumn(
        "attack_direction",
        F.element_at(ad_map, F.struct("match_id", "game_section", "team_id"))
    )

    # Match info map via spark.read.table (dependency visible in UI)
    match_info_df = spark.read.table("`bundesliga-2022-2023`.batch.match_info")
    match_info_df.createOrReplaceTempView("mi")
    mi_map = F.expr("""
        (SELECT map_from_entries(collect_list(struct(
            match_id,
            struct(pitch_x, pitch_y, home_team_id, guest_team_id)
        )))
        FROM mi)
    """)
    bronze_df = bronze_df.withColumn(
        "_mi",
        F.element_at(mi_map, F.col("match_id"))
    ).withColumn(
        "pitch_x", F.col("_mi.pitch_x")
    ).withColumn(
        "pitch_y", F.col("_mi.pitch_y")
    ).withColumn(
        "home_team_id", F.col("_mi.home_team_id")
    ).withColumn(
        "guest_team_id", F.col("_mi.guest_team_id")
    ).drop("_mi")

    # Update ball_possession from 1/2 codes to actual team IDs
    bronze_df = bronze_df.withColumn(
        "ball_possession",
        F.when(F.col("ball_possession") == 1, F.col("home_team_id"))
        .when(F.col("ball_possession") == 2, F.col("guest_team_id"))
        .otherwise(F.lit(None).cast("string"))
    )

    # For BALL rows: resolve attack_direction from the possessing team
    bronze_df = bronze_df.withColumn(
        "attack_direction",
        F.when(F.col("team_id") == "BALL",
            F.element_at(ad_map, F.struct(F.col("match_id"), F.col("game_section"), F.col("ball_possession").alias("team_id")))
        ).otherwise(F.col("attack_direction"))
    )

    # Add x_norm, y_norm (attack-normalized coordinates)
    bronze_df = bronze_df.withColumn(
        "x_norm",
        F.round(
            F.when(
                F.col("attack_direction") == 1,
                F.col("x") + F.col("pitch_x") / 2
            ).otherwise(
                F.col("pitch_x") - (F.col("x") + F.col("pitch_x") / 2)
            ),
            2
        )
    ).withColumn(
        "y_norm",
        F.round(
            F.when(
                F.col("attack_direction") == 1,
                -F.col("y") + F.col("pitch_y") / 2
            ).otherwise(
                F.col("pitch_y") - (-F.col("y") + F.col("pitch_y") / 2)
            ),
            2
        )
    ).drop("pitch_x", "pitch_y", "home_team_id", "guest_team_id")

    return bronze_df
