"""consumer_positions_pairs_v2.py — the Databricks batch pipeline, running on the
streaming architecture of consumer_positions_pairs.py.

WHAT THIS IS
============
A second, independent implementation of the four-stage Databricks batch pipeline
(`pipeline_databricks_batch.zip`) on top of the *existing streaming architecture*:
the same Kafka source, the same foreground `foreachBatch`, the same batched-numpy
`mapInPandas` enrichment, and the same paced background writer / bounded queue.

`consumer_positions_pairs.py` is NOT modified, NOT imported for behaviour, and must
not be run at the same time: both write `…-silver-positions` with different schemas.

    batch stage            batch table                    streaming stage here
    ---------------------  -----------------------------  -----------------------------
    raw_positions           raw_positions                  position_rows()   (src)
    bronze_positions        bronze_positions               bronze_positions()
    silver_positions_...    silver_positions_enrichment    enrich_and_pair_batch()
    silver_positions_group  silver_positions_grouped       grouped_positions()
    gold_possessions        gold_possessions               gold_possessions()

WHAT IS FAITHFUL AND WHAT IS NOT
================================
Faithful to the batch, level by level:
  * the enrichment arithmetic itself -- ball_distance, nearest-possessing-player
    has_possession, mutual-nearest pairing, max(row_min, col_min), target_distance
    against (52.5, 34.0). This is a direct port of the batch's own
    `enrich_and_pair_batch`, with one change: it reads x_norm/y_norm instead of
    the raw X/Y (see below);
  * the normalisation FORMULAS, including the `-y` negation the batch applies for
    attack_direction == 1 (consumer_positions_pipeline.py does NOT negate y; this
    follows the batch, deliberately);
  * output column names, order and types: person_id / x_norm / distance / speed /
    acceleration / ball_possession-as-team-id;
  * rounding: 2 dp on bronze coordinates, 3 dp on silver numerics;
  * grouped view: referee filtered, players sorted ascending by x_norm,
    offside_line = get(players, 1).x_norm, play_state from ball_status;
  * gold: the row_num - row_num_possession sequence trick, the aggregates,
    duration_sec = frames/25, cumulative_time, possession_id, nested structs.

NOT faithful, and necessarily so:
  1. Attack direction. The batch derives it from a whole-match scan of frame ids
     10000 and 100000. A micro-batch does not have those. v2 derives it from the
     EARLIEST frame of each (match_id, game_section) PRESENT IN THE BATCH. That is
     the streaming equivalent of the batch's per-section kickoff frame, and it
     makes the second-half flip automatic -- which is exactly why the batch keys
     the map by game_section. Stateless: nothing is carried between batches.
  2. Possession code -> team id. The batch reads home/guest from match_info, which
     is parsed from the match XML that the batch reads from S3. Nothing in this
     streaming setup produces that. v2 resolves it from a per-match JSON file in
     scripts/.match-info/, falling back to consumer_utils.POSSESSION_TEAM_MAP. An
     UNKNOWN MATCH IS NOT SILENT: ball_possession becomes NULL, the BALL row's
     attack_direction becomes NULL, and the batch logs a warning naming the match.
     has_possession is then wrong for that match -- so `v2-strict-matches` makes it
     fatal instead.
  3. Pitch dimensions. Per-match from the same JSON, else PITCH_LENGTH/PITCH_WIDTH.
  4. Gold possession sequences are BATCH-SCOPED. The batch sees the whole match, so
     one sequence is one possession. In streaming a possession that spans a
     micro-batch boundary is cut in two, and `possession_id` and `cumulative_time`
     restart. Every gold row carries `stream_batch_id` and `sequence_clipped`
     (true when the sequence touches the first or last frame of its batch) so a
     consumer can tell a clipped sequence from a genuine possession change. This
     is the one place where v2 genuinely cannot match the batch.

TOPIC OWNERSHIP -- READ THIS BEFORE ENABLING WRITES
===================================================
By default v2 writes NOTHING (`V2_WRITE_ENABLED=0`) and just measures. Writes were
deliberately made opt-in because the topic names are reused:

    bundesliga-2022-2023-bronze-positions           ALREADY FED BY consumer_positions_pipeline.py
    bundesliga-2022-2023-silver-positions           ALREADY FED BY consumer_positions_pipeline.py
    bundesliga-2022-2023-silver-positions-grouped   new, no collision
    bundesliga-2022-2023-gold-possessions            new; the pipeline consumer writes
                                                      ...-gold-possessions-all instead

The two taken-over topics would receive rows with a DIFFERENT schema from a
different producer. The webui reads silver-positions. So before setting
V2_WRITE_ENABLED=1, STOP consumer_positions_pipeline.py, consumer_positions_lag.py
and consumer_positions_pairs.py. v2 prints that warning at startup every time.

Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
          consumer_positions_pairs_v2.py
"""

import json
import os
import queue
import threading
import time

import numpy as np
import pandas as pd
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


# ── Parameters ──────────────────────────────────────────────────────────────

params = {
    # Declared first: the Kafka sink reads this key, and dict literals evaluate
    # in order.
    "kafka-bootstrap": os.environ.get("V2_KAFKA_BOOTSTRAP", "localhost:9092"),
    "kafka-source-topics": {
        "positions-raw-observed": "bundesliga-2022-2023-raw-positions",
    },
    "kafka-target-topics": {
        "bronze-positions": "bundesliga-2022-2023-bronze-positions",
        "silver-positions": "bundesliga-2022-2023-silver-positions",
        "silver-positions-grouped": "bundesliga-2022-2023-silver-positions-grouped",
        "gold-possessions": "bundesliga-2022-2023-gold-possessions",
    },
    # A checkpoint dir of its own. Sharing consumer_positions_pairs.py's would make
    # the two queries fight over the same offsets if both were ever run.
    "checkpoint-local-dir": os.environ.get(
        "V2_CHECKPOINT_DIR",
        "checkpoint-bundesliga-2022-2023-v2",
    ),
    "processing-time-consumer": 30,

    # Master write switch. 0 = dry run: the full pipeline is planned and executed
    # and every level is counted, but nothing reaches Kafka and the background
    # writer is never started.
    "kafka-write-enabled": int(os.environ.get("V2_WRITE_ENABLED", "0")),

    # Which of the four levels to build at all, and which of those to publish.
    # Building a level always implies building everything above it.
    "v2-levels": {
        s.strip()
        for s in os.environ.get(
            "V2_LEVELS", "bronze,silver,grouped,gold"
        ).split(",")
        if s.strip()
    },
    "v2-publish-levels": {
        s.strip()
        for s in os.environ.get(
            "V2_PUBLISH_LEVELS", "bronze,silver,grouped,gold"
        ).split(",")
        if s.strip()
    },

    # Paced write, on a background thread, spread across the time left before the
    # next trigger. Same mechanism and same reasoning as consumer_positions_pairs:
    # the batch sink has no throttle, so .save() lands a whole micro-batch in a
    # fraction of a second and then the topic goes silent for the rest of the
    # window.
    "kafka-write-paced": int(os.environ.get("V2_WRITE_PACED", "1")),

    # The enrichment groups by (match_id, frame_id) inside one partition, so every
    # row of a frame MUST co-locate. repartition(n, col) keeps the co-location
    # (same hash key) while capping the task count.
    "enrich-partitions": int(os.environ.get("V2_ENRICH_PARTITIONS", "8")),
    "bronze-partitions": int(os.environ.get("V2_BRONZE_PARTITIONS", "4")),
    "gold-partitions": int(os.environ.get("V2_GOLD_PARTITIONS", "4")),

    "write-pull-partitions": int(
        os.environ.get("V2_WRITE_PULL_PARTITIONS", "4")
    ),

    # Backpressure: when the writer falls behind the micro-batch blocks on put()
    # instead of queueing unbounded batches in driver memory.
    "writer-queue-capacity": int(os.environ.get("V2_WRITER_QUEUE", "2")),
    "writer-min-window": float(os.environ.get("V2_WRITER_MIN_WINDOW", "2.0")),
    "writer-flush-margin": float(
        os.environ.get("V2_WRITER_FLUSH_MARGIN", "0.5")
    ),

    # Batch parity: gold duration_sec = num_frames / FPS. The tracking data is 25
    # fps, same constant as gold/gold_possessions.py.
    "fps": 25.0,

    # The batch's offside line is get(players, 1).x_norm on an array sorted
    # ascending by x_norm -- i.e. the SECOND-deepest player. Index 0 would be the
    # deepest, which is what consumer_positions_pipeline.py uses. Default 1 keeps
    # v2 batch-faithful; set V2_OFFSIDE_INDEX=0 to get the deepest instead.
    "offside-line-index": int(os.environ.get("V2_OFFSIDE_INDEX", "1")),

    # An unknown match in POSSESSION_TEAM_MAP / .match-info silently corrupts
    # ball_possession and the BALL row's attack_direction. 1 = raise instead.
    "strict-matches": int(os.environ.get("V2_STRICT_MATCHES", "0")),

    # Per-batch audit of rows whose attack_direction could not be resolved. Two
    # small actions per micro-batch, so it is the one deliberately eager step.
    # 0 once the direction derivation has been trusted on real data.
    "audit": int(os.environ.get("V2_AUDIT", "1")),
}

VALID_LEVELS = ("bronze", "silver", "grouped", "gold")


def level_enabled(name):
    return name in params["v2-levels"]


def level_published(name):
    return (
        params["kafka-write-enabled"]
        and name in params["v2-levels"]
        and name in params["v2-publish-levels"]
    )


input_df = None
spark = None


# ── Schemas ─────────────────────────────────────────────────────────────────
#
# Defined here rather than imported: the batch's silver schema is NOT the one in
# scripts/schemas.py. That one carries player_id / X / Y / D / S / A and an INTEGER
# ball_possession, which is the streaming-native shape. The batch carries
# person_id / x_norm / y_norm / distance / speed / acceleration and a STRING
# ball_possession holding the team id. Mixing them up is the whole point of this
# file, so it is written out in full.

BRONZE_SCHEMA = T.StructType([
    T.StructField("match_id", T.StringType(), True),
    T.StructField("game_section", T.StringType(), True),
    T.StructField("frame_id", T.LongType(), True),
    T.StructField("team_id", T.StringType(), True),
    T.StructField("person_id", T.StringType(), True),
    T.StructField("timestamp", T.StringType(), True),
    T.StructField("x", T.DoubleType(), True),
    T.StructField("y", T.DoubleType(), True),
    T.StructField("distance", T.DoubleType(), True),
    T.StructField("speed", T.DoubleType(), True),
    T.StructField("acceleration", T.DoubleType(), True),
    T.StructField("m_flag", T.IntegerType(), True),
    T.StructField("ball_possession", T.StringType(), True),
    T.StructField("ball_status", T.IntegerType(), True),
    T.StructField("attack_direction", T.IntegerType(), True),
    T.StructField("x_norm", T.DoubleType(), True),
    T.StructField("y_norm", T.DoubleType(), True),
])

SILVER_POSITIONS_SCHEMA = T.StructType([
    T.StructField("match_id", T.StringType(), True),
    T.StructField("game_section", T.StringType(), True),
    T.StructField("frame_id", T.LongType(), True),
    T.StructField("team_id", T.StringType(), True),
    T.StructField("person_id", T.StringType(), True),
    T.StructField("x_norm", T.DoubleType(), True),
    T.StructField("y_norm", T.DoubleType(), True),
    T.StructField("distance", T.DoubleType(), True),
    T.StructField("speed", T.DoubleType(), True),
    T.StructField("acceleration", T.DoubleType(), True),
    T.StructField("ball_possession", T.StringType(), True),
    T.StructField("ball_status", T.IntegerType(), True),
    T.StructField("ball_distance", T.DoubleType(), True),
    T.StructField("has_possession", T.BooleanType(), True),
    T.StructField("pair_player_id", T.StringType(), True),
    T.StructField("pair_player_distance", T.DoubleType(), True),
    T.StructField("closest_opponent_distance", T.DoubleType(), True),
    T.StructField("target_distance", T.DoubleType(), True),
])

SILVER_OUTPUT_COLUMNS = [f.name for f in SILVER_POSITIONS_SCHEMA.fields]

BRONZE_OUTPUT_COLUMNS = [f.name for f in BRONZE_SCHEMA.fields]

# The nested struct array the grouped view carries, in the batch's exact order.
_GROUPED_PLAYER_COLUMNS = [
    "person_id", "speed", "distance", "acceleration",
    "x_norm", "y_norm",
    "ball_possession", "ball_status",
    "ball_distance", "has_possession",
    "pair_player_id", "pair_player_distance",
    "closest_opponent_distance", "target_distance",
]

GROUPED_SCHEMA = T.StructType([
    T.StructField("match_id", T.StringType(), True),
    T.StructField("team_id", T.StringType(), True),
    T.StructField("frame_id", T.LongType(), True),
    T.StructField("game_section", T.StringType(), True),
    T.StructField("players", T.ArrayType(
        T.StructType([
            T.StructField("person_id", T.StringType(), True),
            T.StructField("speed", T.DoubleType(), True),
            T.StructField("distance", T.DoubleType(), True),
            T.StructField("acceleration", T.DoubleType(), True),
            T.StructField("x_norm", T.DoubleType(), True),
            T.StructField("y_norm", T.DoubleType(), True),
            T.StructField("ball_possession", T.StringType(), True),
            T.StructField("ball_status", T.IntegerType(), True),
            T.StructField("ball_distance", T.DoubleType(), True),
            T.StructField("has_possession", T.BooleanType(), True),
            T.StructField("pair_player_id", T.StringType(), True),
            T.StructField("pair_player_distance", T.DoubleType(), True),
            T.StructField("closest_opponent_distance", T.DoubleType(), True),
            T.StructField("target_distance", T.DoubleType(), True),
        ])
    ), True),
    T.StructField("offside_line", T.DoubleType(), True),
    T.StructField("play_state", T.StringType(), True),
])

GROUPED_OUTPUT_COLUMNS = [
    "match_id", "team_id", "frame_id", "game_section",
    "players", "offside_line", "play_state",
]

GOLD_SCHEMA = T.StructType([
    T.StructField("possession_id", T.LongType(), True),
    T.StructField("match_id", T.StringType(), True),
    T.StructField("game_section", T.StringType(), True),
    T.StructField("cumulative_time", T.StringType(), True),
    T.StructField("team_id", T.StringType(), True),
    T.StructField("opponent_id", T.StringType(), True),
    T.StructField("start_frame", T.LongType(), True),
    T.StructField("end_frame", T.LongType(), True),
    T.StructField("num_frames", T.LongType(), True),
    T.StructField("duration_sec", T.DoubleType(), True),
    T.StructField("duration_min", T.DoubleType(), True),
    T.StructField("active_frames", T.LongType(), True),
    T.StructField("interruption_frames", T.LongType(), True),
    T.StructField("team_metrics", T.StructType([
        T.StructField("avg_ball_speed", T.DoubleType(), True),
        T.StructField("avg_offside_line", T.DoubleType(), True),
    ]), True),
    T.StructField("opponent_metrics", T.StructType([
        T.StructField("avg_offside_line", T.DoubleType(), True),
    ]), True),
    # Streaming-only. The batch has no need for these because it sees the whole
    # match and a sequence is therefore never clipped; see the module docstring.
    T.StructField("stream_batch_id", T.LongType(), True),
    T.StructField("sequence_clipped", T.BooleanType(), True),
])

GOLD_OUTPUT_COLUMNS = [f.name for f in GOLD_SCHEMA.fields]

TARGET_POINT = (52.5, 34.0)


# ── Match metadata ──────────────────────────────────────────────────────────
#
# The batch's bronze joins match_info, parsed from the match XML, to get
# home_team_id / guest_team_id / pitch_x / pitch_y. There is no producer of that
# topic anywhere in scripts/, so v2 assembles the same four values from, in order:
#
#   1. scripts/.match-info/<match_id>.json -- drop a file in to make a match work
#      without touching any code:
#        {"home_team_id": "DFL-CLU-000008", "guest_team_id": "DFL-CLU-00000G",
#         "pitch_x": 105.0, "pitch_y": 68.0}
#   2. consumer_utils.POSSESSION_TEAM_MAP (7 matches, some entries only "[xml only]")
#   3. PITCH_LENGTH / PITCH_WIDTH from consumer_utils for the pitch dimensions.

MATCH_INFO_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".match-info"
)

_HOME_TEAM = {}
_GUEST_TEAM = {}
_PITCH_X = {}
_PITCH_Y = {}
_UNKNOWN_MATCHES_WARNED = set()


def load_match_info():
    """Build the four per-match maps. Called once, before the query starts."""
    global _HOME_TEAM, _GUEST_TEAM, _PITCH_X, _PITCH_Y

    from consumer_utils import (
        POSSESSION_TEAM_MAP,
        PITCH_LENGTH,
        PITCH_WIDTH,
    )

    home, guest, px, py = {}, {}, {}, {}

    if os.path.isdir(MATCH_INFO_DIR):
        for name in sorted(os.listdir(MATCH_INFO_DIR)):
            if not name.endswith(".json"):
                continue
            path = os.path.join(MATCH_INFO_DIR, name)
            try:
                with open(path, encoding="utf-8") as handle:
                    doc = json.load(handle)
            except (OSError, ValueError) as exc:
                print(f"[v2] WARNING unreadable {path}: {exc}")
                continue
            match_id = doc.get("match_id") or os.path.splitext(name)[0]
            home[match_id] = doc.get("home_team_id")
            guest[match_id] = doc.get("guest_team_id")
            px[match_id] = float(doc.get("pitch_x", PITCH_LENGTH))
            py[match_id] = float(doc.get("pitch_y", PITCH_WIDTH))
        print(f"[v2] match-info: {len(home)} match(es) from {MATCH_INFO_DIR}")

    for match_id, slots in POSSESSION_TEAM_MAP.items():
        home.setdefault(match_id, slots.get(1))
        guest.setdefault(match_id, slots.get(2))
        px.setdefault(match_id, float(PITCH_LENGTH))
        py.setdefault(match_id, float(PITCH_WIDTH))

    _HOME_TEAM, _GUEST_TEAM, _PITCH_X, _PITCH_Y = home, guest, px, py
    print(f"[v2] match metadata: {len(home)} match(es) known")


def _string_map_expr(mapping, key_col):
    """A folded map<string,string> lookup, or a typed NULL when the map is empty.

    create_map needs an even argument list, so an empty dict cannot become one.
    element_at over an empty map is what makes an unknown match a clean NULL
    instead of an error.
    """
    if not mapping:
        return F.lit(None).cast("map<string,string>")
    args = []
    for k, v in sorted(mapping.items()):
        args.extend([F.lit(k), F.lit(v)])
    return F.element_at(F.create_map(*args), key_col)


def _double_map_expr(mapping, key_col, default):
    if not mapping:
        return F.lit(None).cast("map<string,double>")
    args = []
    for k, v in sorted(mapping.items()):
        args.extend([F.lit(k), F.lit(float(v))])
    return F.coalesce(
        F.element_at(F.create_map(*args), key_col), F.lit(float(default))
    )


def _warn_unknown_matches(matches):
    """Name any match whose home/guest the pipeline could not resolve."""
    if not matches:
        return
    fresh = sorted(set(matches) - _UNKNOWN_MATCHES_WARNED)
    _UNKNOWN_MATCHES_WARNED.update(matches)
    if not fresh:
        return
    message = ", ".join(fresh)
    print(
        f"[v2] WARNING unknown match(es): {message}\n"
        f"[v2]   ball_possession is NULL and the BALL row has no attack_direction,\n"
        f"[v2]   so has_possession and y_norm are wrong for them. Add a file at\n"
        f"[v2]   {MATCH_INFO_DIR}/<match_id>.json or extend POSSESSION_TEAM_MAP."
    )
    if params["strict-matches"]:
        raise RuntimeError(
            f"[v2] V2_STRICT_MATCHES=1 and unknown match(es): {message}"
        )


def _resolve_match_metadata(df):
    """Attach pitch_x / pitch_y columns, warning once per unknown match."""
    from consumer_utils import PITCH_LENGTH, PITCH_WIDTH

    df = (
        df.withColumn(
            "pitch_x", _double_map_expr(_PITCH_X, F.col("match_id"), PITCH_LENGTH)
        )
        .withColumn(
            "pitch_y", _double_map_expr(_PITCH_Y, F.col("match_id"), PITCH_WIDTH)
        )
    )
    unknown = [
        row["match_id"]
        for row in df.filter(F.col("match_id").isNotNull())
        .select("match_id").distinct().collect()
        if row["match_id"] not in _HOME_TEAM
        or row["match_id"] not in _GUEST_TEAM
    ]
    _warn_unknown_matches(unknown)
    return df


# ── RAW stage ───────────────────────────────────────────────────────────────
#
# The batch reads the time-major S3 JSON and explodes `frames`. producer.py has
# already done that explode and published one message per entity per frame, so
# the only difference here is the projection. It uses POSITIONS_SCHEMA (not the
# PROJECTION variant) so that Frame.T and Frame.M survive -- the batch's raw keeps
# both as `timestamp` and `m_flag`, and consumer_positions_pairs.py drops them.


def position_rows(batch_df):
    from schemas import POSITIONS_SCHEMA

    return (
        batch_df
        .filter(F.col("topic") == params["kafka-source-topics"]["positions-raw-observed"])
        .select(
            # Arrival time of the raw Kafka record, carried through every stage as
            # _arrival_ms purely to pace the writer. Never written to a topic.
            F.unix_millis(F.col("timestamp")).cast("long").alias("_arrival_ms"),
            F.from_json(
                F.col("value").cast("string"), POSITIONS_SCHEMA
            ).alias("position"),
        )
        .select(
            F.col("_arrival_ms"),
            F.col("position.FrameSet.MatchId").alias("match_id"),
            F.col("position.FrameSet.GameSection").alias("game_section"),
            F.col("position.Frame.N").cast("long").alias("frame_id"),
            F.col("position.FrameSet.TeamId").alias("team_id"),
            F.col("position.FrameSet.PersonId").alias("person_id"),
            # Kept as the raw ISO-8601 string rather than cast to a timestamp:
            # same instant, no session-timezone round-trip to get wrong. The
            # session is UTC, so either is equivalent here.
            F.col("position.Frame.T").alias("timestamp"),
            F.col("position.Frame.X").cast("double").alias("x"),
            F.col("position.Frame.Y").cast("double").alias("y"),
            F.col("position.Frame.D").cast("double").alias("distance"),
            F.col("position.Frame.S").cast("double").alias("speed"),
            F.col("position.Frame.A").cast("double").alias("acceleration"),
            F.col("position.Frame.M").cast("int").alias("m_flag"),
            F.col("position.Frame.BallPossession").cast("int").alias(
                "ball_possession_code"
            ),
            F.col("position.Frame.BallStatus").cast("int").alias("ball_status"),
        )
    )


# ── BRONZE stage ────────────────────────────────────────────────────────────
#
# The batch's attack direction comes from a scalar subquery over
# `WHERE frame_id IN (10000, 100000)`: one frame per game_section, the kickoff of
# each half, so keying the map by game_section makes the second-half flip
# automatic. A streaming micro-batch cannot ask for those frame ids -- in a 30s
# window it holds ~750 consecutive frames and neither kickoff frame is in it.
#
# So v2 uses the same idea against what the batch actually has: the EARLIEST frame
# of each (match_id, game_section) present in this micro-batch. Same per-section
# seeding, same map key, same CASE expression, and the same half-time behaviour --
# the second half is seeded from its own first frame and comes out with its own
# direction. Nothing is carried between batches, so a half-time boundary that
# falls mid-batch is handled by keying on game_section (a batch spanning both
# halves seeds both directions and each row picks up its own).
#
# The heuristic is the batch's own and is weak: a team spread across the pitch has
# a near-symmetric x range whatever it is doing, so the sign is decided by one or
# two outlying players (a keeper, a full-back) in that single frame. Kept for
# parity, not because it is good.


def _ensure_arrival(df):
    """Guarantee an _arrival_ms column so every stage can be paced.

    position_rows() always produces one, so this only matters when a stage is
    driven from somewhere else (tests, a replay harness).
    """
    if df is None:
        return df
    if "_arrival_ms" not in df.columns:
        return df.withColumn("_arrival_ms", F.lit(None).cast("long"))
    return df


def bronze_positions(rows):
    rows = _ensure_arrival(rows)
    players = rows.filter(
        ~F.col("team_id").isin("BALL", "referee")
        & F.col("x").isNotNull()
    )

    # The kickoff frame of each half, as far as this batch is concerned.
    kickoff = players.groupBy(
        "match_id", "game_section"
    ).agg(F.min("frame_id").alias("kickoff_frame_id"))

    seeded = (
        players
        .join(F.broadcast(kickoff), on=["match_id", "game_section"])
        .filter(F.col("frame_id") == F.col("kickoff_frame_id"))
    )

    # Batch: CASE WHEN abs(min_x) > abs(max_x) THEN 1 ELSE -1 END
    direction_map = seeded.groupBy(
        "match_id", "game_section", "team_id"
    ).agg(
        F.when(
            F.abs(F.min("x")) > F.abs(F.max("x")), F.lit(1)
        ).otherwise(F.lit(-1)).alias("attack_direction")
    )

    bronzed = rows.join(
        F.broadcast(direction_map),
        on=["match_id", "game_section", "team_id"],
        how="left",
    )

    # Possession code -> team id, in the data, exactly as the batch's bronze does.
    # 1 = home, 2 = guest, anything else NULL.
    match_col = F.col("match_id")
    home = _string_map_expr(_HOME_TEAM, match_col)
    guest = _string_map_expr(_GUEST_TEAM, match_col)
    bronzed = bronzed.withColumn(
        "possession_team",
        F.when(F.col("ball_possession_code") == 1, home)
        .when(F.col("ball_possession_code") == 2, guest)
        .otherwise(F.lit(None).cast("string")),
    )

    # For BALL rows the direction is the possessing team's, since "BALL" is not a
    # key in the direction map. The batch does the same lookup; coalesce leaves
    # every non-BALL row's own direction untouched.
    ball_direction = (
        direction_map
        .withColumnRenamed("team_id", "possession_team")
        .withColumnRenamed("attack_direction", "ball_attack_direction")
    )
    bronzed = (
        bronzed
        .join(
            F.broadcast(ball_direction),
            on=["match_id", "game_section", "possession_team"],
            how="left",
        )
        .withColumn(
            "attack_direction",
            F.coalesce(
                F.col("attack_direction"), F.col("ball_attack_direction")
            ),
        )
    )

    bronzed = _resolve_match_metadata(bronzed)

    # The batch's normalisation, verbatim -- including the -y for
    # attack_direction == 1, which consumer_positions_pipeline.py does NOT do.
    # Both coordinates land in [0, pitch] with +x pointing at the goal the team is
    # attacking.
    direction = F.col("attack_direction")
    half_x = F.col("pitch_x") / F.lit(2.0)
    half_y = F.col("pitch_y") / F.lit(2.0)

    bronzed = (
        bronzed
        .withColumn(
            "x_norm",
            F.round(
                F.when(direction == 1, F.col("x") + half_x)
                .otherwise(F.col("pitch_x") - (F.col("x") + half_x)),
                2,
            ),
        )
        .withColumn(
            "y_norm",
            F.round(
                F.when(direction == 1, -F.col("y") + half_y)
                .otherwise(F.col("pitch_y") - (-F.col("y") + half_y)),
                2,
            ),
        )
        .withColumn("ball_possession", F.col("possession_team"))
        .select(*BRONZE_OUTPUT_COLUMNS, "_arrival_ms")
    )
    return bronzed


def _bronze_audit(df):
    """Rows whose attack_direction could not be resolved, by match.

    Worth surfacing because the failure is silent in the data: a NULL direction
    sends x_norm/y_norm down the `otherwise` branch, which is a perfectly
    ordinary-looking number in [0, pitch].
    """
    bad = (
        df.filter(F.col("attack_direction").isNull())
        .groupBy("match_id")
        .agg(
            F.count(F.lit(1)).alias("rows"),
            F.min("frame_id").alias("first_frame"),
            F.max("frame_id").alias("last_frame"),
        )
        .collect()
    )
    for row in bad:
        print(
            f"[v2] WARNING no attack_direction for match {row['match_id']}: "
            f"{row['rows']} rows, frames {row['first_frame']}-{row['last_frame']}"
        )
    return len(bad)


# ── SILVER stage ────────────────────────────────────────────────────────────
#
# A direct port of the batch's silver/silver_enrichment.py::enrich_and_pair_batch,
# keeping v1's batched-numpy structure so it does not pay the per-frame pandas
# cost. Two differences from the batch's version, both deliberate:
#
#   * it reads x_norm / y_norm, because in the batch the enrichment sits directly
#     on bronze and bronze is already normalised. In consumer_positions_pairs.py
#     the same function reads raw X / Y, which is precisely the misalignment the
#     report found: same formula, different coordinate frame.
#   * ball_possession already holds a team id, so there is no possession_team_map
#     to resolve it through.
#
# The rounding to 3 dp is applied in the Spark select below, not in pandas, so the
# numpy output stays full precision exactly as the batch's does.

_BATCHED_REQUIRED = frozenset({
    "match_id", "game_section", "frame_id", "team_id", "person_id",
    "x_norm", "y_norm", "distance", "speed", "acceleration",
    "ball_possession", "ball_status",
})


def _is_missing_scalar(value):
    return value is None or (
        isinstance(value, float) and np.isnan(value)
    )


def empty_enriched_frame():
    return pd.DataFrame(columns=SILVER_OUTPUT_COLUMNS)


def enrich_and_pair_batch(batch_iter, target_point=None):
    frames = [f for f in batch_iter if f is not None and not f.empty]
    if not frames:
        yield empty_enriched_frame()
        return
    if not all(_BATCHED_REQUIRED.issubset(f.columns) for f in frames):
        raise RuntimeError(
            "[v2] enrichment input is missing columns the batched path needs: "
            f"{sorted(_BATCHED_REQUIRED)}"
        )

    df = pd.concat(frames, ignore_index=True, copy=False)
    total = len(df)

    # ---- every column read exactly once, before the frame loop ------------
    match_ids = df["match_id"].to_numpy(dtype=object)
    team_ids = df["team_id"].to_numpy(dtype=object)
    person_ids = df["person_id"].to_numpy(dtype=object)
    frame_ids = df["frame_id"].to_numpy(dtype=np.int64)
    game_sections = df["game_section"].to_numpy(dtype=object)
    x_all = df["x_norm"].to_numpy(dtype=np.float64, na_value=np.nan)
    y_all = df["y_norm"].to_numpy(dtype=np.float64, na_value=np.nan)
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

    if target_point is None:
        target_point = TARGET_POINT
    try:
        target_x, target_y = float(target_point[0]), float(target_point[1])
    except (TypeError, ValueError, IndexError):
        target_x, target_y = TARGET_POINT

    finite = np.isfinite(x_all) & np.isfinite(y_all)
    with np.errstate(invalid="ignore", over="ignore"):
        target_distance = np.sqrt(
            (x_all - target_x) ** 2 + (y_all - target_y) ** 2
        )
    target_distance[~finite] = np.nan

    person_present = np.fromiter(
        (not _is_missing_scalar(v) for v in person_ids), bool, total
    )
    team_present = np.fromiter(
        (not _is_missing_scalar(v) for v in team_ids), bool, total
    )
    is_ball = team_ids == "BALL"
    excluded = is_ball | (team_ids == "referee")

    for group in range(group_count):
        rows = row_order[bounds[group]:bounds[group + 1]]
        gx, gy = x_all[rows], y_all[rows]
        g_finite = finite[rows]
        g_team, g_person = team_ids[rows], person_ids[rows]

        ball_rows = np.flatnonzero(is_ball[rows])
        if ball_rows.size:
            ball_row = int(ball_rows[0])
            bx, by = gx[ball_row], gy[ball_row]
            if np.isfinite(bx) and np.isfinite(by):
                with np.errstate(invalid="ignore", over="ignore"):
                    distances_to_ball = np.sqrt(
                        (gx - bx) ** 2 + (gy - by) ** 2
                    )
                distances_to_ball[~g_finite] = np.nan
                ball_distance[rows] = distances_to_ball
                # In the batch this value is already the team id, mapped in bronze.
                possession_team = possession_raw[rows[ball_row]]
                if not _is_missing_scalar(possession_team):
                    candidates = np.flatnonzero(
                        (g_team == possession_team)
                        & person_present[rows]
                        & np.isfinite(distances_to_ball)
                    )
                    if candidates.size:
                        nearest = candidates[
                            np.argmin(distances_to_ball[candidates])
                        ]
                        has_possession[rows[nearest]] = True

        person_mask = ~excluded[rows] & person_present[rows] & g_finite
        partner_map = distance_map = closest_map = None
        if person_mask.any():
            selected = rows[person_mask]
            p_team, p_person = team_ids[selected], person_ids[selected]

            # First two teams in order of first appearance. The batch uses a `seen`
            # dict for this rather than pd.unique, and so does v1.
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
                    for person_id, distance in zip(p_person[first], row_min):
                        closest_map[person_id] = distance
                    for person_id, distance in zip(p_person[second], col_min):
                        closest_map[person_id] = distance

                    sorted_rows, sorted_cols = np.unravel_index(
                        np.argsort(distances, axis=None, kind="stable"),
                        distances.shape,
                    )
                    used_rows = np.zeros(first.size, dtype=bool)
                    used_cols = np.zeros(second.size, dtype=bool)
                    max_pairs = min(first.size, second.size)
                    # The scan skips rejected pairs, so the accepted ones sit at
                    # arbitrary positions in the sorted arrays. Collect them as they
                    # are accepted; taking the first max_pairs entries would pick up
                    # rejected pairs and change the result.
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
                            for person_a, person_b, distance in zip(
                                p_person[first][pair_rows[keep]],
                                p_person[second][pair_cols[keep]],
                                pair_distances[keep],
                            ):
                                partner_map[person_a] = person_b
                                partner_map[person_b] = person_a
                                distance_map[person_a] = distance
                                distance_map[person_b] = distance

        # One pass over the frame's rows.
        if partner_map is not None or closest_map is not None:
            for position, row in enumerate(rows):
                person_id = g_person[position]
                if _is_missing_scalar(person_id):
                    continue
                if partner_map is not None:
                    partner[row] = partner_map.get(person_id)
                    partner_distance[row] = distance_map.get(person_id, np.nan)
                if closest_map is not None:
                    closest_opponent[row] = closest_map.get(person_id, np.nan)

    sequence = np.concatenate(
        [row_order[bounds[g]:bounds[g + 1]] for g in range(group_count)]
    )
    result = pd.DataFrame({
        "match_id": match_ids[sequence],
        "game_section": game_sections[sequence],
        "frame_id": frame_ids[sequence],
        "team_id": team_ids[sequence],
        "person_id": person_ids[sequence],
        "x_norm": x_all[sequence],
        "y_norm": y_all[sequence],
        "distance": d_all[sequence],
        "speed": s_all[sequence],
        "acceleration": a_all[sequence],
        # No dtype wrapper here, exactly as the batch: the column arrives as an
        # object array of team ids and pandas infers. Wrapping it in
        # pd.array(..., dtype="string") changes the dtype (not the values), which
        # Spark then converts anyway, but it stops the output being bit-identical.
        "ball_possession": possession_raw[sequence],
        "ball_status": pd.array(status_raw[sequence], dtype="Int64"),
        "ball_distance": ball_distance[sequence],
        "has_possession": has_possession[sequence],
        "pair_player_id": partner[sequence],
        "pair_player_distance": partner_distance[sequence],
        "closest_opponent_distance": closest_opponent[sequence],
        "target_distance": target_distance[sequence],
    })
    yield result.loc[:, SILVER_OUTPUT_COLUMNS]


def silver_positions(bronzed, batch_id):
    """Bronze -> flat silver, with the batch's 3-decimal rounding."""
    return (
        bronzed
        # The batch's silver_enrichment() input filter: identity columns must be
        # present, and a row without a person_id is only kept if it is the ball or
        # the referee, since those have no person.
        .filter(
            F.col("match_id").isNotNull()
            & F.col("frame_id").isNotNull()
            & F.col("team_id").isNotNull()
            & (
                F.col("person_id").isNotNull()
                | F.col("team_id").isin("BALL", "referee")
            )
        )
        .repartition(int(params["enrich-partitions"]), F.col("frame_id"))
        .mapInPandas(enrich_and_pair_batch, schema=SILVER_POSITIONS_SCHEMA)
        .select(
            "match_id", "game_section", "frame_id", "team_id", "person_id",
            F.round("x_norm", 3).alias("x_norm"),
            F.round("y_norm", 3).alias("y_norm"),
            F.round("distance", 3).alias("distance"),
            F.round("speed", 3).alias("speed"),
            F.round("acceleration", 3).alias("acceleration"),
            "ball_possession", "ball_status",
            F.round("ball_distance", 3).alias("ball_distance"),
            "has_possession",
            "pair_player_id",
            F.round("pair_player_distance", 3).alias("pair_player_distance"),
            F.round("closest_opponent_distance", 3).alias(
                "closest_opponent_distance"
            ),
            F.round("target_distance", 3).alias("target_distance"),
        )
        .withColumn("stream_batch_id", F.lit(batch_id).cast("long"))
    )


# ── GROUPED SILVER stage ────────────────────────────────────────────────────
#
# The batch's silver_positions_grouped, on the group that the enrichment just
# produced rather than on a stored table.


def grouped_positions(silver):
    silver = _ensure_arrival(silver)
    grouped = (
        silver
        .filter(F.col("team_id") != "referee")
        .groupBy("match_id", "team_id", "frame_id", "game_section")
        .agg(
            F.max("stream_batch_id").alias("stream_batch_id"),
            F.max("_arrival_ms").alias("_arrival_ms"),
            F.collect_list(
                F.struct(
                    *[F.col(c).alias(c) for c in _GROUPED_PLAYER_COLUMNS]
                )
            ).alias("players"),
        )
        # Ascending by x_norm, so index 0 is the DEEPEST attacker.
        .withColumn(
            "players",
            F.expr(
                "array_sort(players, (a, b) -> CASE "
                "WHEN a.x_norm < b.x_norm THEN -1 "
                "WHEN a.x_norm > b.x_norm THEN 1 ELSE 0 END)"
            ),
        )
        .withColumn(
            "offside_line",
            F.expr(f"get(players, {int(params['offside-line-index'])}).x_norm"),
        )
    )

    # play_state is a per-FRAME value repeated on each team row, taken from the
    # BALL group's single entry.
    w_frame = Window.partitionBy("match_id", "frame_id")
    grouped = grouped.withColumn(
        "play_state",
        F.when(
            F.max(
                F.when(
                    F.col("team_id") == "BALL",
                    F.expr("get(players, 0).ball_status"),
                ).otherwise(F.lit(None))
            ).over(w_frame) == 1,
            "active",
        ).otherwise("interruption"),
    )

    return grouped


# ── GOLD stage ──────────────────────────────────────────────────────────────
#
# The batch's gold_possessions, with the two columns added that streaming needs.


def gold_possessions(grouped, batch_id):
    grouped = _ensure_arrival(grouped)
    w_frame = Window.partitionBy("match_id", "frame_id")

    all_team_frames = (
        grouped
        .withColumn(
            "_poss_team",
            F.max(
                F.when(
                    F.col("team_id") == "BALL",
                    F.expr("get(players, 0).ball_possession"),
                ).otherwise(F.lit(None))
            ).over(w_frame),
        )
        .filter(F.col("team_id") != "BALL")
        .withColumn(
            "has_possession", F.col("_poss_team") == F.col("team_id")
        )
        .drop("_poss_team")
        .withColumn(
            "_opp_offside_line",
            F.max(
                F.when(
                    F.col("has_possession") == False, F.col("offside_line")
                ).otherwise(F.lit(None))
            ).over(w_frame),
        )
        .withColumn(
            "_opp_team_id",
            F.max(
                F.when(
                    F.col("has_possession") == False, F.col("team_id")
                ).otherwise(F.lit(None))
            ).over(w_frame),
        )
    )

    possession_frames = all_team_frames.filter(F.col("has_possession") == True)

    # The batch's stateless sequence id: the difference between a frame's rank
    # within the half and its rank within that team's possessions. Constant while
    # one team keeps the ball, and jumps when the ball changes hands.
    possession_frames = (
        possession_frames
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("match_id", "game_section").orderBy("frame_id")
            ),
        )
        .withColumn(
            "row_num_possession",
            F.row_number().over(
                Window.partitionBy("match_id", "game_section", "team_id")
                .orderBy("frame_id")
            ),
        )
        .withColumn("group_id", F.col("row_num") - F.col("row_num_possession"))
    )

    fps = F.lit(float(params["fps"]))
    sequences = (
        possession_frames
        .groupBy("match_id", "game_section", "team_id", "group_id")
        .agg(
            F.min("frame_id").alias("start_frame"),
            F.max("frame_id").alias("end_frame"),
            F.countDistinct("frame_id").alias("num_frames"),
            F.sum(
                F.when(F.col("play_state") == "active", 1).otherwise(0)
            ).alias("active_frames"),
            F.sum(
                F.when(F.col("play_state") == "interruption", 1).otherwise(0)
            ).alias("interruption_frames"),
            # NB: players[0] of an outfield team group is that team's DEEPEST
            # player, not the ball -- the ball is the separate BALL group. So this
            # field is an outfield player's speed. Kept verbatim for parity with
            # the batch; consumer_positions_pipeline.py reads ball_positions.* and
            # is measuring something different under the same name.
            F.round(
                F.avg(
                    F.when(
                        F.expr("get(players, 0).distance") > 0,
                        F.expr("get(players, 0).speed"),
                    )
                ),
                2,
            ).alias("avg_ball_speed"),
            F.round(F.avg("offside_line"), 2).alias("avg_offside_line"),
            F.round(
                F.avg("_opp_offside_line"), 2
            ).alias("opponent_avg_offside_line"),
            F.max("_opp_team_id").alias("opponent_id"),
        )
        .withColumn("duration_sec", F.col("num_frames") / fps)
        .withColumn(
            "duration_min", F.round(F.col("duration_sec") / 60, 2)
        )
        .withColumn(
            "cumulative_time",
            _cumulative_time_expr(
                Window.partitionBy("match_id", "game_section").orderBy("start_frame")
            ),
        )
        .withColumn(
            "possession_id",
            F.row_number().over(
                Window.partitionBy("match_id", "game_section").orderBy("start_frame")
            ),
        )
        .withColumn(
            "team_metrics",
            F.struct(
                F.col("avg_ball_speed").alias("avg_ball_speed"),
                F.col("avg_offside_line").alias("avg_offside_line"),
            ),
        )
        .withColumn(
            "opponent_metrics",
            F.struct(
                F.col("opponent_avg_offside_line").alias("avg_offside_line"),
            ),
        )
        .withColumn("stream_batch_id", F.lit(batch_id).cast("long"))
        # A sequence is CLIPPED when it reaches either edge of the batch's frame
        # range for its half: it may continue into the previous or the next
        # micro-batch. Anything strictly inside both edges is bounded by a real
        # possession change on both sides, which is the only case where the batch
        # and v2 agree exactly.
        #
        # Computed over the AGGREGATED sequences, so it must read start_frame /
        # end_frame rather than frame_id -- frame_id no longer exists after the
        # groupBy. min(start_frame) per (match, game_section) is the batch's own
        # first frame, max(end_frame) its last.
        .withColumn(
            "_batch_first_frame",
            F.min("start_frame").over(
                Window.partitionBy("match_id", "game_section")
            ),
        )
        .withColumn(
            "_batch_last_frame",
            F.max("end_frame").over(
                Window.partitionBy("match_id", "game_section")
            ),
        )
        .withColumn(
            "sequence_clipped",
            (F.col("start_frame") <= F.col("_batch_first_frame"))
            | (F.col("end_frame") >= F.col("_batch_last_frame")),
        )
    )

    return (
        sequences
        .select(
            "possession_id", "match_id", "game_section", "cumulative_time",
            "team_id", "opponent_id",
            "start_frame", "end_frame", "num_frames",
            "duration_sec", "duration_min",
            "active_frames", "interruption_frames",
            "team_metrics", "opponent_metrics",
            "stream_batch_id", "sequence_clipped",
        )
    )


def _cumulative_time_expr(window):
    """Running possession time as MM:SS, per the batch."""
    running = F.sum("duration_sec").over(
        window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    return F.concat(
        F.lpad(
            F.floor(running / 60).cast("int").cast("string"), 2, "0"
        ),
        F.lit(":"),
        F.lpad(
            F.floor(running % 60).cast("int").cast("string"), 2, "0"
        ),
    )


# ── Kafka framing and the paced writer ──────────────────────────────────────
#
# Unchanged in behaviour from consumer_positions_pairs.py: one writer thread for
# the life of the job, a bounded queue as the backpressure, and rows published
# spread across the time left before the next trigger. The only difference is that
# four levels share the queue instead of one, so each level is enqueued with its
# own topic.


def kafka_frame(df, columns, arrival_col="_arrival_ms"):
    """key / value / _arrival_ms, ready for the writer.

    Only `columns` reaches the JSON, so `_arrival_ms` and `stream_batch_id` can
    ride along for pacing and lineage without leaking into the payload.
    """
    key_col = "match_id" if "match_id" in columns else None
    return df.select(
        *([] if key_col is None else [F.col(key_col).cast("string").alias("key")]),
        F.to_json(F.struct(*[F.col(c) for c in columns])).alias("value"),
        F.col(arrival_col).alias("_arrival_ms"),
    )


_producer = None


def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers=params["kafka-bootstrap"],
            linger_ms=5,
            batch_size=65536,
            compression_type="lz4",
            acks=1,
        )
    return _producer


_writer_thread = None
_writer_queue = None


def writer_loop():
    """Publish queued micro-batches across the time left before the next trigger.

    window = next_trigger_start - now - flush_margin, floored at
    writer-min-window. Measured from dequeue, not enqueue: if the writer was still
    draining the previous batch, part of the window is gone and the remaining rows
    are squeezed into what is left.

    Wait first, then send. The deadline is each row's own arrival time mapped onto
    the window, and it is absolute from `started` rather than chained to the
    previous deadline, so a late send does not stretch the gap and later deadlines
    are simply already in the past, which makes the loop self-correcting.
    """
    producer = get_producer()
    min_window = float(params["writer-min-window"])
    flush_margin = float(params["writer-flush-margin"])

    while True:
        item = _writer_queue.get()
        if item is None:
            return

        topic, rows, first_arrival, last_arrival, next_trigger = item
        span_ms = (
            0 if first_arrival is None or last_arrival is None
            else last_arrival - first_arrival
        )
        started = time.perf_counter()
        window = max(
            (next_trigger or 0.0) - started - flush_margin, min_window
        )

        for arrival, key_bytes, value_bytes in rows:
            if span_ms > 0 and arrival is not None:
                fraction = (arrival - first_arrival) / float(span_ms)
                delay = (started + window * fraction) - time.perf_counter()
                if delay > 0:
                    time.sleep(delay)
            producer.send(topic, key=key_bytes, value=value_bytes)

        producer.flush()


def ensure_writer():
    global _writer_thread, _writer_queue
    if _writer_thread is None:
        _writer_queue = queue.Queue(
            maxsize=int(params["writer-queue-capacity"])
        )
        _writer_thread = threading.Thread(
            target=writer_loop, name="v2-writer", daemon=True
        )
        _writer_thread.start()


def submit_to_writer(topic, rows, first_arrival, last_arrival, next_trigger):
    ensure_writer()
    _writer_queue.put((topic, rows, first_arrival, last_arrival, next_trigger))
    return len(rows)


def enqueue_kafka_paced(frame, topic, next_trigger):
    """Collect the batch and hand it to the writer, returning immediately.

    Collect rather than toLocalIterator: the writer needs the whole batch in hand
    anyway, and toLocalIterator makes one RPC per partition. A 30s window is
    ~19 500 rows, so this holds ~8 MB briefly on the driver.

    sortWithinPartitions, not orderBy. A global orderBy adds a range shuffle on
    _arrival_ms, which pushes the to_json projection into a separate stage
    downstream of the enrichment instead of fusing into its output. Sorting inside
    the partitions the enrichment already produced adds no Exchange at all; the
    global order is completed on the driver below.
    """
    ordered = frame.sortWithinPartitions(
        F.col("_arrival_ms").asc_nulls_last()
    )
    collected = [
        (
            row["_arrival_ms"],
            None if row.get("key") is None else row["key"].encode("utf-8"),
            row["value"].encode("utf-8"),
        )
        for row in ordered.collect()
    ]
    if not collected:
        return 0

    # Merge the per-partition runs into one globally ordered list. list.sort is
    # stable, so rows sharing an arrival -- the 26 rows of one frame -- keep their
    # relative order and stay adjacent for the writer.
    collected.sort(key=lambda row: (1, 0) if row[0] is None else (0, row[0]))

    arrivals = [a for a, _, _ in collected if a is not None]
    first_arrival = min(arrivals) if arrivals else None
    last_arrival = max(arrivals) if arrivals else None
    return submit_to_writer(
        topic, collected, first_arrival, last_arrival, next_trigger
    )


def emit(df, level, output_columns, next_trigger, batch_id, stats):
    """Publish one level, or count it in a dry run.

    The count() in the dry-run branch is the point: a Spark DataFrame is lazy, so
    returning without an action would execute nothing, report zero input rows and
    measure nothing.
    """
    topic_key = {
        "bronze": "bronze-positions",
        "silver": "silver-positions",
        "grouped": "silver-positions-grouped",
        "gold": "gold-possessions",
    }[level]
    topic = params["kafka-target-topics"][topic_key]

    if not level_published(level):
        count = 0 if df is None else df.count()
        stats[level] = stats.get(level, 0) + count
        return count

    framed = kafka_frame(_ensure_arrival(df), output_columns)
    if params["kafka-write-paced"]:
        written = enqueue_kafka_paced(framed, topic, next_trigger)
    else:
        framed.localCheckpoint(eager=True).write.format("kafka").option(
            "kafka.bootstrap.servers", params["kafka-bootstrap"]
        ).option("topic", topic).save()
        written = -1
    stats[level] = stats.get(level, 0) + written
    return written


# ── Micro-batch ─────────────────────────────────────────────────────────────


def process_batch(batch_df, batch_id):
    # Invoked BY the trigger, so this is the trigger's start. The next one follows
    # exactly one trigger interval later, which is what bounds the writer's
    # publish window: enrichment time subtracts itself from the window
    # automatically.
    trigger_started = time.perf_counter()
    stats = {}

    rows = position_rows(batch_df)
    if not level_enabled("bronze"):
        return

    bronzed = bronze_positions(rows)

    # Direction failures are silent in the data -- a NULL direction takes the
    # `otherwise` branch and produces an ordinary-looking coordinate -- so they
    # are counted here, once per batch, before anything is published.
    #
    # This is the one deliberately eager step: two small actions per micro-batch.
    # V2_AUDIT=0 removes them once the direction derivation has been trusted on
    # real data.
    unmatched = _bronze_audit(bronzed) if params["audit"] else -1

    next_trigger = (
        trigger_started + float(params["processing-time-consumer"])
    )

    # Each level depends on the one above it, so a level that is switched off is
    # still COMPUTED when a lower level needs it -- only its publish is skipped.
    silver = None
    if level_enabled("silver"):
        silver = silver_positions(bronzed, batch_id)
        emit(
            silver, "silver", SILVER_OUTPUT_COLUMNS, next_trigger, batch_id, stats
        )

    want_grouped = level_enabled("grouped")
    want_gold = level_enabled("gold")

    if want_grouped or want_gold:
        # The grouped view is built from the ENRICHED silver, so when silver is
        # off it has to be computed anyway even though nothing publishes it.
        source = silver if silver is not None else silver_positions(
            bronzed, batch_id
        )
        grouped = grouped_positions(source)

        if want_grouped:
            emit(
                grouped, "grouped", GROUPED_OUTPUT_COLUMNS, next_trigger,
                batch_id, stats,
            )

        if want_gold:
            gold = gold_possessions(grouped, batch_id)
            emit(
                gold, "gold", GOLD_OUTPUT_COLUMNS, next_trigger, batch_id, stats
            )

    elapsed = time.perf_counter() - trigger_started
    print(
        f"[v2] batch {batch_id}: bronze={stats.get('bronze', 0)} "
        f"silver={stats.get('silver', 0)} grouped={stats.get('grouped', 0)} "
        f"gold={stats.get('gold', 0)} "
        f"no_direction_rows={unmatched} in {elapsed:.1f}s"
    )


# ── Session and stream ──────────────────────────────────────────────────────


def main():
    global input_df, spark
    spark = (
        SparkSession.builder
        .appName("bundesliga-2022-2023-pairs-v2")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    # Log level deliberately NOT overridden, so spark-submit's INFO applies and the
    # per-stage/per-task metrics stay visible.
    input_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", params["kafka-bootstrap"])
        .option(
            "subscribe", ",".join(params["kafka-source-topics"].values())
        )
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )


def banner():
    print("=" * 72)
    print("consumer_positions_pairs_v2 -- Databricks batch on the streaming stack")
    print("=" * 72)
    print(f"  levels built     : {sorted(params['v2-levels'])}")
    print(f"  levels published : {sorted(params['v2-publish-levels'])}")
    print(f"  offside line     : get(players, {params['offside-line-index']})"
          f"  (batch parity = 1)")
    print(f"  y_norm           : batch formula, -y negated for direction == 1")
    print(f"  paced write      : {bool(params['kafka-write-paced'])}")
    if not params["kafka-write-enabled"]:
        print("  WRITE            : DISABLED (V2_WRITE_ENABLED=0) -- dry run, "
              "every level is counted and nothing reaches Kafka")
    else:
        for level, key in (
            ("bronze", "bronze-positions"),
            ("silver", "silver-positions"),
            ("grouped", "silver-positions-grouped"),
            ("gold", "gold-possessions"),
        ):
            if level in params["v2-publish-levels"]:
                print(f"  -> {level:8s} {params['kafka-target-topics'][key]}")
        print()
        print("  *** V2_WRITE_ENABLED=1 ***")
        print("  bundesliga-2022-2023-bronze-positions and -silver-positions are")
        print("  ALSO fed by consumer_positions_pipeline.py with a DIFFERENT schema.")
        print("  STOP consumer_positions_pipeline.py, consumer_positions_lag.py and")
        print("  consumer_positions_pairs.py first, or the webui will read a mix of")
        print("  both schemas.")
    print("=" * 72)


def run_stream():
    banner()
    # main() FIRST. It builds the SparkSession, and that has to exist before
    # load_match_info(): consumer_utils.py builds its ATTACKING_DIRECTION /
    # HALF_START_FRAME maps with F.lit() at module scope, and in Spark 4.x
    # F.lit() asserts on a missing active SparkContext. The banner first so the
    # configuration is visible even if session creation fails.
    main()
    load_match_info()
    query = (
        input_df
        .writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", params["checkpoint-local-dir"])
        .trigger(
            processingTime=f"{params['processing-time-consumer']} seconds"
        )
        .start()
    )
    try:
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    run_stream()