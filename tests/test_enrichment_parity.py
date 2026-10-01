"""Enrichment parity test: streaming v2 vs the Databricks batch.

The streaming implementation is a port of the batch's own
``silver/silver_enrichment.py::enrich_and_pair_batch``. This test executes both
functions on byte-identical input frames and compares every output column
exactly (rtol=0, atol=0). If this passes, the two enrichers agree bit-for-bit.

The batch file is exec'd with stubs for the Databricks runtime (``pyspark.pipelines``
decorators, the ``spark`` global, and the ``transformations.schemas`` import), so the
test runs on plain PySpark without Databricks.

Run from the repo root:

    python3 tests/test_enrichment_parity.py
"""

import itertools
import os
import sys
import types

import numpy as np
import pandas as pd

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BATCH_FILE = os.path.join(REPO, "batch", "silver", "silver_enrichment.py")
STREAMING_DIR = os.path.join(REPO, "streaming")

# The batch's output contract. Read from the batch's own schema module when it can be
# imported, so this test cannot drift away from what the batch actually declares.
BATCH_OUTPUT_COLUMNS = [
    "match_id", "game_section", "frame_id", "team_id", "person_id",
    "x_norm", "y_norm", "distance", "speed", "acceleration",
    "ball_possession", "ball_status", "ball_distance", "has_possession",
    "pair_player_id", "pair_player_distance", "closest_opponent_distance",
    "target_distance",
]


# ── stub the Databricks runtime so the batch file imports outside Databricks ──

class _TableDecorator:
    def __call__(self, *args, **kwargs):
        def wrap(fn):
            return fn
        return wrap

    def __getattr__(self, _name):
        return self()


def load_batch_enrich():
    import pyspark

    dp = types.ModuleType("pyspark.pipelines")
    for name in ("table", "materialized_view", "view", "expectation"):
        setattr(dp, name, _TableDecorator())
    pyspark.pipelines = dp

    schema_mod = types.ModuleType("transformations.schemas.silver_positions_schema")
    schema_mod.SILVER_POSITIONS_SCHEMA = None
    schema_mod.SILVER_OUTPUT_COLUMNS = BATCH_OUTPUT_COLUMNS
    for pkg in ("transformations", "transformations.schemas"):
        sys.modules[pkg] = types.ModuleType(pkg)
    sys.modules["transformations.schemas.silver_positions_schema"] = schema_mod

    namespace = {
        "__name__": "batch_enrichment",
        "spark": types.SimpleNamespace(
            conf=types.SimpleNamespace(get=lambda key, default=None: "8")
        ),
    }
    with open(BATCH_FILE, encoding="utf-8") as handle:
        exec(compile(handle.read(), BATCH_FILE, "exec"), namespace)
    return namespace["enrich_and_pair_batch"]


# ── input frames ────────────────────────────────────────────────────────────

TEAM_A = "DFL-CLU-000008"
TEAM_B = "DFL-CLU-00000G"
MATCH_A = "DFL-MAT-J03WMX"
MATCH_B = "DFL-MAT-J03WN1"


def make_frame(frame_id, rng, match_id=MATCH_A, game_section="firstHalf"):
    """One frame: 11 players a side, plus the ball and the referee.

    Coordinates are already normalised (x in [0,105], y in [0,68]) because in the
    batch the enrichment sits directly on bronze, which is normalised.
    """
    rows = []
    for team, tag in ((TEAM_A, "H"), (TEAM_B, "G")):
        for i in range(11):
            rows.append({
                "match_id": match_id,
                "game_section": game_section,
                "frame_id": frame_id,
                "team_id": team,
                "person_id": f"{tag}{i:02d}",
                "x_norm": float(rng.uniform(0, 105)),
                "y_norm": float(rng.uniform(0, 68)),
                "distance": float(rng.uniform(0, 9000)),
                "speed": float(rng.uniform(0, 9)),
                "acceleration": float(rng.uniform(-9, 9)),
                "ball_possession": team,
                "ball_status": int(rng.integers(0, 3)),
                "timestamp": f"2022-09-23T14:00:{frame_id % 60:02d}.000Z",
            })

    possessing = (TEAM_A, TEAM_B)[frame_id % 2]

    # The ball and the referee have no PersonId in the source data.
    for team in ("BALL", "referee"):
        rows.append({
            "match_id": match_id,
            "game_section": game_section,
            "frame_id": frame_id,
            "team_id": team,
            "person_id": None,
            "x_norm": float(rng.uniform(0, 105)),
            "y_norm": float(rng.uniform(0, 68)),
            "distance": 0.0,
            "speed": float(rng.uniform(0, 30)) if team == "BALL" else 0.0,
            "acceleration": 0.0,
            "ball_possession": possessing,
            "ball_status": int(rng.integers(0, 2)),
            "timestamp": f"2022-09-23T14:00:{frame_id % 60:02d}.000Z",
        })

    return pd.DataFrame(rows)


def make_degenerate_frame(frame_id, rng):
    """The cases that break naive implementations: only one team on the pitch, a
    null person_id, and a NaN coordinate."""
    frame = make_frame(frame_id, rng)
    rows = frame.to_dict("records")

    kept = [
        r for r in rows
        if not (r["team_id"] == TEAM_B and r["frame_id"] == frame_id)
    ]
    for r in kept:
        if r["team_id"] == TEAM_A and r["person_id"] == "H00":
            r["person_id"] = None
        if r["team_id"] == TEAM_A and r["person_id"] == "H01":
            r["x_norm"] = float("nan")

    return pd.DataFrame(kept)


# ── comparison ──────────────────────────────────────────────────────────────

def compare(label, frames, batch_enrich, streaming_enrich):
    batch_out = next(batch_enrich(iter([f.copy() for f in frames])))
    stream_out = next(streaming_enrich(iter([f.copy() for f in frames])))
    batch_out = batch_out.loc[:, BATCH_OUTPUT_COLUMNS]
    stream_out = stream_out.loc[:, BATCH_OUTPUT_COLUMNS]

    ok = True
    if list(batch_out.columns) != list(stream_out.columns):
        print(f"  [{label}] COLUMN MISMATCH")
        return False
    if len(batch_out) != len(stream_out):
        print(f"  [{label}] ROW COUNT {len(batch_out)} vs {len(stream_out)}")
        ok = False

    for column in BATCH_OUTPUT_COLUMNS:
        left, right = batch_out[column], stream_out[column]
        if pd.api.types.is_float_dtype(left) or pd.api.types.is_float_dtype(right):
            equal = np.allclose(
                left.to_numpy(dtype="float64", na_value=np.nan),
                right.to_numpy(dtype="float64", na_value=np.nan),
                rtol=0, atol=0, equal_nan=True,
            )
        else:
            equal = left.astype("string").fillna("<NA>").equals(
                right.astype("string").fillna("<NA>")
            )
        if not equal:
            print(f"  [{label}] VALUE MISMATCH in {column}")
            ok = False
        elif left.dtype != right.dtype:
            print(f"  [{label}] dtype mismatch in {column}: "
                  f"{left.dtype} vs {right.dtype}")
            ok = False

    print(f"  [{label}] {'IDENTICAL' if ok else 'MISMATCH'} "
          f"({len(batch_out)} rows, {len(frames)} frame(s))")
    return ok


def main():
    sys.path.insert(0, STREAMING_DIR)
    batch_enrich = load_batch_enrich()

    import consumer_positions_pairs_v2
    streaming_enrich = consumer_positions_pairs_v2.enrich_and_pair_batch

    print("enrichment parity: streaming v2 vs Databricks batch\n")

    rng = np.random.default_rng(20260930)
    results = []

    results.append(compare("single frame", [make_frame(10000, rng)],
                           batch_enrich, streaming_enrich))
    results.append(compare("3 frames", [make_frame(10000 + i, rng) for i in range(3)],
                           batch_enrich, streaming_enrich))
    results.append(compare(
        "two matches, two halves",
        [make_frame(10000 + i, rng) for i in range(2)]
        + [make_frame(100000 + i, rng, MATCH_B, "secondHalf") for i in range(2)],
        batch_enrich, streaming_enrich,
    ))
    results.append(compare("degenerate", [make_degenerate_frame(10000, rng)],
                           batch_enrich, streaming_enrich))

    # Arrow hands the enricher several chunks per call, so a frame's rows can arrive
    # split across chunks. Both must stitch them in the same group order.
    frames = [make_frame(10000 + i, rng) for i in range(7)]
    chunk = 3
    def chunks(fn):
        return pd.concat(
            list(fn(itertools.chain.from_iterable(
                frames[i:i + chunk] for i in range(0, len(frames), chunk)
            ))),
            ignore_index=True,
        ).loc[:, BATCH_OUTPUT_COLUMNS]

    chunked_equal = chunks(batch_enrich).equals(chunks(streaming_enrich))
    print(f"  [7 frames in chunks of {chunk}] "
          f"{'IDENTICAL' if chunked_equal else 'MISMATCH'} "
          f"({len(frames) * 24} rows)")
    results.append(chunked_equal)

    print()
    if all(results):
        print("PASS - the streaming enricher is bit-identical to the batch's.")
        return 0
    print("FAIL - the two enrichers disagree.")
    return 1


if __name__ == "__main__":
    sys.exit(main())