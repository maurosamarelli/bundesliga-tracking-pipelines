from pyspark import pipelines as dp
from pyspark.sql import functions as F
import numpy as np
import pandas as pd

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
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
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
    "x", "y", "x_norm", "y_norm", "distance", "speed", "acceleration",
    "ball_possession", "ball_status",
    "ball_distance", "has_possession",
    "pair_player_id", "pair_player_distance",
    "closest_opponent_distance", "target_distance",
]

TARGET_POINT_NORM = (105, 34)
ENRICH_PARTITIONS = int(spark.conf.get("enrich_partitions", "200"))


def _is_missing_scalar(v):
    return v is None or (isinstance(v, float) and np.isnan(v))


def empty_enriched_frame():
    return pd.DataFrame(columns=SILVER_OUTPUT_COLUMNS)


def enrich_and_pair_batch(
    batch_iter,
):
    """Batched-numpy enrichment reading from bronze_positions.

    Computes per-frame: ball_distance, has_possession, mutual-nearest
    partner pairing, closest_opponent_distance, and target_distance.
    """
    frames = [f for f in batch_iter if f is not None and not f.empty]
    if not frames:
        yield empty_enriched_frame()
        return

    df = pd.concat(frames, ignore_index=True, copy=False)
    total = len(df)

    # ---- every column read exactly once, before the frame loop ------------
    match_ids = df["match_id"].to_numpy(dtype=object)
    team_ids = df["team_id"].to_numpy(dtype=object)
    player_ids = df["person_id"].to_numpy(dtype=object)
    frame_ids = df["frame_id"].to_numpy(dtype=np.int64)
    x_all = df["x"].to_numpy(dtype=np.float64, na_value=np.nan)
    y_all = df["y"].to_numpy(dtype=np.float64, na_value=np.nan)
    x_norm_all = df["x_norm"].to_numpy(dtype=np.float64, na_value=np.nan)
    y_norm_all = df["y_norm"].to_numpy(dtype=np.float64, na_value=np.nan)
    possession_all = df["ball_possession"].to_numpy(dtype=object)
    source_ts = df["timestamp"].to_numpy()
    game_sections = df["game_section"].to_numpy(dtype=object)
    d_all = df["distance"].to_numpy(dtype=np.float64, na_value=np.nan)
    s_all = df["speed"].to_numpy(dtype=np.float64, na_value=np.nan)
    a_all = df["acceleration"].to_numpy(dtype=np.float64, na_value=np.nan)
    possession_raw = df["ball_possession"].to_numpy()
    status_raw = df["ball_status"].to_numpy()

    # ---- group by (match_id, frame_id), first-appearance order -------------
    match_code, _ = pd.factorize(match_ids, sort=False)
    group_code, group_keys = pd.factorize(
        match_code.astype(np.int64) * (1 << 40) + frame_ids, sort=False
    )
    group_count = len(group_keys)
    if group_count == 0:
        yield empty_enriched_frame()
        return
    row_order = np.argsort(group_code, kind="stable")
    bounds = np.concatenate(
        [[0], np.cumsum(np.bincount(group_code, minlength=group_count))]
    )

    ball_distance = np.full(total, np.nan, dtype=np.float64)
    has_possession = np.zeros(total, dtype=bool)
    partner = np.full(total, None, dtype=object)
    partner_distance = np.full(total, np.nan, dtype=np.float64)
    closest_opponent = np.full(total, np.nan, dtype=np.float64)

    target_x, target_y = float(TARGET_POINT_NORM[0]), float(TARGET_POINT_NORM[1])

    finite = np.isfinite(x_norm_all) & np.isfinite(y_norm_all)
    with np.errstate(invalid="ignore", over="ignore"):
        target_distance = np.sqrt(
            (x_norm_all - target_x) ** 2 + (y_norm_all - target_y) ** 2
        )
    target_distance[~finite] = np.nan

    match_present = np.fromiter(
        (not _is_missing_scalar(v) for v in match_ids), bool, total
    )
    player_present = np.fromiter(
        (not _is_missing_scalar(v) for v in player_ids), bool, total
    )
    is_ball = team_ids == "BALL"
    excluded = is_ball | (team_ids == "referee")

    for group in range(group_count):
        rows = row_order[bounds[group]:bounds[group + 1]]
        gx, gy = x_all[rows], y_all[rows]
        g_finite = finite[rows]
        g_team, g_player = team_ids[rows], player_ids[rows]

        ball_rows = np.flatnonzero(is_ball[rows])
        if ball_rows.size:
            ball_row = int(ball_rows[0])
            # Forward-fill ball_possession and ball_status from BALL row to all rows in frame
            bp = possession_all[rows[ball_row]]
            possession_team = None if _is_missing_scalar(bp) else bp
            if possession_team is not None:
                possession_raw[rows] = possession_team
            bs_val = status_raw[rows[ball_row]]
            if not pd.isna(bs_val):
                status_raw[rows] = bs_val
            bx, by = gx[ball_row], gy[ball_row]
            if np.isfinite(bx) and np.isfinite(by):
                with np.errstate(invalid="ignore", over="ignore"):
                    distances_to_ball = np.sqrt(
                        (gx - bx) ** 2 + (gy - by) ** 2
                    )
                distances_to_ball[~g_finite] = np.nan
                ball_distance[rows] = distances_to_ball
                if possession_team is not None:
                    candidates = np.flatnonzero(
                        (g_team == possession_team)
                        & player_present[rows]
                        & np.isfinite(distances_to_ball)
                    )
                    if candidates.size:
                        nearest = candidates[
                            np.argmin(distances_to_ball[candidates])
                        ]
                        has_possession[rows[nearest]] = True

        player_mask = ~excluded[rows] & player_present[rows] & g_finite
        partner_map = distance_map = closest_map = None
        if player_mask.any():
            selected = rows[player_mask]
            p_team, p_player = team_ids[selected], player_ids[selected]

            # first two teams in order of first appearance
            seen = {}
            for team in p_team:
                if team not in seen:
                    seen[team] = None
            team_names = list(seen)
            if len(team_names) >= 2:
                first = np.flatnonzero(p_team == team_names[0])
                second = np.flatnonzero(p_team == team_names[1])
                if first.size and second.size:
                    px, py = x_all[selected], y_all[selected]
                    ax, ay = px[first], py[first]
                    bx2, by2 = px[second], py[second]
                    distances = np.sqrt(
                        (ax[:, None] - bx2[None, :]) ** 2
                        + (ay[:, None] - by2[None, :]) ** 2
                    )
                    row_min = distances.min(axis=1)
                    col_min = distances.min(axis=0)

                    closest_map = {}
                    for player_id, distance in zip(p_player[first], row_min):
                        closest_map[player_id] = distance
                    for player_id, distance in zip(p_player[second], col_min):
                        closest_map[player_id] = distance

                    sorted_rows, sorted_cols = np.unravel_index(
                        np.argsort(distances, axis=None, kind="stable"),
                        distances.shape,
                    )
                    used_rows = np.zeros(first.size, dtype=bool)
                    used_cols = np.zeros(second.size, dtype=bool)
                    max_pairs = min(first.size, second.size)
                    accepted_rows = []
                    accepted_cols = []
                    for row, col in zip(sorted_rows, sorted_cols):
                        if used_rows[row] or used_cols[col]:
                            continue
                        used_rows[row] = used_cols[col] = True
                        accepted_rows.append(row)
                        accepted_cols.append(col)
                        if len(accepted_rows) == max_pairs:
                            break

                    if accepted_rows:
                        pair_rows = np.asarray(accepted_rows, dtype=np.intp)
                        pair_cols = np.asarray(accepted_cols, dtype=np.intp)
                        pair_distances = distances[pair_rows, pair_cols]
                        keep = (
                            (pair_distances == row_min[pair_rows])
                            & (pair_distances == col_min[pair_cols])
                        )
                        if keep.any():
                            partner_map, distance_map = {}, {}
                            for player_a, player_b, distance in zip(
                                p_player[first][pair_rows[keep]],
                                p_player[second][pair_cols[keep]],
                                pair_distances[keep],
                            ):
                                partner_map[player_a] = player_b
                                partner_map[player_b] = player_a
                                distance_map[player_a] = distance
                                distance_map[player_b] = distance

        # one pass over the frame's rows
        if partner_map is not None or closest_map is not None:
            for position, row in enumerate(rows):
                player_id = g_player[position]
                if _is_missing_scalar(player_id):
                    continue
                if partner_map is not None:
                    partner[row] = partner_map.get(player_id)
                    partner_distance[row] = distance_map.get(player_id, np.nan)
                if closest_map is not None:
                    closest_opponent[row] = closest_map.get(player_id, np.nan)

    sequence = np.concatenate(
        [row_order[bounds[g]:bounds[g + 1]] for g in range(group_count)]
    )
    result = pd.DataFrame({
        "match_id": match_ids[sequence],
        "game_section": game_sections[sequence],
        "frame_id": frame_ids[sequence],
        "team_id": team_ids[sequence],
        "person_id": player_ids[sequence],
        "x": np.round(x_all[sequence], 3),
        "y": np.round(y_all[sequence], 3),
        "x_norm": np.round(x_norm_all[sequence], 3),
        "y_norm": np.round(y_norm_all[sequence], 3),
        "distance": np.round(d_all[sequence], 3),
        "speed": np.round(s_all[sequence], 3),
        "acceleration": np.round(a_all[sequence], 3),
        "ball_possession": possession_raw[sequence],
        "ball_status": pd.array(status_raw[sequence], dtype="Int64"),
        "ball_distance": np.round(ball_distance[sequence], 3),
        "has_possession": has_possession[sequence],
        "pair_player_id": partner[sequence],
        "pair_player_distance": np.round(partner_distance[sequence], 3),
        "closest_opponent_distance": np.round(closest_opponent[sequence], 3),
        "target_distance": np.round(target_distance[sequence], 3),
    })
    yield result.loc[:, SILVER_OUTPUT_COLUMNS]

@dp.materialized_view(
    name="`bundesliga-2022-2023`.batch.silver_positions_enrichment",
    comment="Per-frame enrichment from bronze_positions: ball distance, possession, partner pairing, closest opponent, target distance",
    partition_cols=["match_id"],
)
def silver_enrichment():
    positions = spark.read.table("`bundesliga-2022-2023`.batch.bronze_positions")

    return (
        positions
        .filter(
            F.col("match_id").isNotNull()
            & F.col("frame_id").isNotNull()
            & F.col("team_id").isNotNull()
            & F.col("person_id").isNotNull()
            & (F.col("team_id") != "referee")
        )
        .repartition(ENRICH_PARTITIONS, F.col("frame_id"))
        .mapInPandas(
            enrich_and_pair_batch,
            schema=SILVER_POSITIONS_SCHEMA,
        )
    )

