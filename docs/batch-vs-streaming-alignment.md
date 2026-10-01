# Batch (Databricks) vs Streaming (Kafka) alignment report

**Date:** 2026-09-30
**Batch source:** `pipeline_databricks_batch.zip` (extracted read-only to `/tmp/opencode/batch`)
**Streaming source of truth for the comparison:** `scripts/consumer_positions_pairs.py`
**Reference only (final section):** the other consumers in `scripts/`

> No file under `scripts/` was modified. This report is the only artefact produced.

---

## 0. Scope and method

| Side | What was read |
|---|---|
| Batch | `raw/raw_positions.py`, `raw/match_info.py`, `bronze/bronze_positions.py`, `silver/silver_positions.py`, `silver/silver_enrichment.py`, `schemas/silver_positions_schema.py`, `gold/gold_possessions.py` |
| Streaming | `scripts/consumer_positions_pairs.py` (1292 lines), `scripts/schemas.py`, `scripts/consumer_utils.py`, `scripts/producer.py` |

Levels compared: **raw → bronze → silver → gold**.

For the streaming side, `consumer_positions_pairs.py` produces exactly **one** output stage
(`…-silver-positions`, plus a side `…-pairs` debug topic). It has **no bronze and no gold stage**.
So for those two levels the comparison is "batch has it, streaming does not" and the question
becomes *where the equivalent logic does live* (answered in §7).

---

## 1. Verdict summary

| Level | Aligned | Not aligned | Verdict |
|---|---|---|---|
| **RAW** | source file format, explode semantics, key columns, typing of D/S/A/X/Y, possession code meaning | timestamp handling, `M`/`Z` columns, entity vs frame row grain, `ball_possession` typing | **Mostly aligned** (one real loss: no match timestamp) |
| **BRONZE** | *nothing present in streaming* | attack-direction derivation, coordinate normalisation, possession code → team id, pitch dimensions | **Not aligned** — streaming has no bronze stage at all |
| **SILVER** | enrichment algorithm (ball distance, possession, mutual-nearest pairing, closest opponent, target distance) — line-for-line equivalent | coordinate system, column names, `ball_possession` type, rounding, timestamp/lineage, referee filtering | **Calculations aligned, schema not aligned** |
| **GOLD** | *nothing present in streaming* | possession sequencing, offside line, play state, cumulative time, possession id | **Not aligned** — streaming has no gold stage (logic exists elsewhere, but differs in detail) |

The single most important finding is in **silver**: the enrichment maths is the same code, but it is
evaluated in **two different coordinate systems**, so `target_distance`, `ball_distance`,
`pair_player_distance` and `closest_opponent_distance` are **not comparable between the two pipelines**.

---

## 2. RAW level

### 2.1 Source

Both sides consume the **same reshaped time-major JSONL** (`{"n": …, "frames": [ … ]}`,
one line per frame). The producer reads `input/<match>.jsonl`, which is the offline reshape of
`s3://bundesliga-2022-2023-data/timemajor/`; the batch pipeline reads that same S3 prefix directly
with `cloudFiles`.

- Batch: `spark.readStream … .load("s3://…/timemajor/")` then `.select(n as frame_id, explode(frames))`
- Streaming: `producer.py` explodes in Python (`for frame in frames: producer.send(…)`) and publishes
  one Kafka message per **entity per frame**, keyed by `team_id`.

**Aligned:** same upstream data, same explode operation (different engine), same `frame_id = n`,
same `match_id` / `game_section` / `team_id` / `person_id` extraction from `FrameSet`, same
`BallPossession` / `BallStatus` codes from `Frame`.

### 2.2 Not aligned

| Item | Batch | Streaming pairs consumer | Impact |
|---|---|---|---|
| **Timestamp** | `to_timestamp(Frame.T)` → `timestamp` column | **not projected at all.** `POSITIONS_PROJECTION_SCHEMA` (`schemas.py:45`) has no `T` | Streaming silver has **no match clock**. `source_timestamp` is the *Kafka arrival time* (`consumer_positions_pairs.py:894-903`), which is a transport artifact, not a match time. The webui therefore has to re-derive frame→clock itself. |
| **`M` (m_flag)** | selected as `m_flag` | present in `POSITIONS_SCHEMA` but **dropped** from `POSITIONS_PROJECTION_SCHEMA` | Lost before enrichment; neither silver carries it. |
| **`Z`** | selected | never projected | Lost. Batch keeps it in raw only; neither silver has it. |
| **Grain** | one row per entity per frame | one Kafka message per entity per frame | Same grain. **Aligned.** |
| **Partition key** | none | `key = team_id`, explicit partition | Cosmetic; no cross-entity ordering is relied upon downstream. |

### 2.3 Aligned (explicit)

- `x`,`y`,`z`,`D`,`S`,`A` all cast to `double` in both.
- `BallPossession` / `BallStatus` cast to `int` in both.
- `inferColumnTypes = true` (batch) vs an explicit DDL projection (streaming) produce the same types.

**RAW verdict:** source and typing aligned; the only substantive loss is `Frame.T`.

---

## 3. BRONZE level — **entirely absent from streaming**

The batch bronze (`bronze/bronze_positions.py`) does four things. `consumer_positions_pairs.py`
does **none** of them.

### 3.1 Attack direction

Batch derives it **from data**, per `(match_id, game_section, team_id)`:

```python
CASE WHEN abs(min_x) > abs(max_x) THEN 1 ELSE -1 END
… WHERE frame_id IN (10000, 100000) …
```

Because the key includes `game_section`, the second-half flip is **automatic** — the second half is
measured on its own first frame (`n = 100000`), so no half-time boundary constant is needed.

Streaming: no bronze stage at all. The equivalent exists in
`consumer_positions_pipeline.py:250-296`, but the direction comes from
**`consumer_utils.ATTACKING_DIRECTION`** (`consumer_utils.py:111`) — a hard-coded `create_map`
containing **exactly one match, `DFL-MAT-J03WMX`** — and the second half is handled by
**`HALF_START_FRAME`** (`consumer_utils.py:135`), also hard-coded to `100000`, also one match.

| | Batch | Streaming (`consumer_positions_pipeline.py`) |
|---|---|---|
| Direction source | measured from positions | hard-coded map |
| Matches covered | all (any match in S3) | 1 of 7 |
| 2nd-half flip | implicit (per game_section) | explicit `frame_id >= 100000` constant |
| Unknown match | derived | `element_at` returns `NULL` → `x_norm` silently falls to the `otherwise` branch |
| Heuristic quality | weak (see below) | weak (inherited) |

**Robustness note (a batch-side weakness, not a mismatch):** `abs(min_x) > abs(max_x)` computed
over *all 22 players in one frame* is not a reliable direction test — a team spread across the whole
pitch has a near-symmetric x range whatever it is doing, so the sign is decided by one or two
outlying players (a keeper, a full-back) in that single frame. The streaming constant, although
hard-coded, is at least *verified* against real data for the one match it covers.

### 3.2 Coordinate normalisation

Batch bronze:

```python
x_norm = round( when(dir == 1, x + pitch_x/2).otherwise(pitch_x - (x + pitch_x/2)), 2 )   #  = x + px/2  or  px/2 - x
y_norm = round( when(dir == 1, -y + pitch_y/2).otherwise(pitch_y - (-y + pitch_y/2)), 2 ) #  = py/2 - y  or  py/2 + y
```

Streaming pipeline (`consumer_positions_pipeline.py:297-310`):

```python
x_norm = round( when(dir == 1, x + 105/2).otherwise(105 - (x + 105/2)), 2 )   #  same algebra
y_norm = round( when(dir == 1,  y +  68/2).otherwise( 68 - ( y +  68/2)), 2 )  #  = y + py/2  or  py/2 - y
```

Two differences:

1. **The `y` sign is inverted.** Batch negates `y` for `dir == 1`; streaming does not.
   Batch `y_norm(dir=1) = py/2 - y`; streaming `y_norm(dir=1) = py/2 + y`.
   The two `y_norm` columns are **mirror images of each other** for exactly one of the two teams,
   and identical for the other — i.e. they are not reconcilable by a single global transform.
2. **Pitch dimensions.** Batch takes `PitchX`/`PitchY` from `match_info` (parsed from the match XML);
   streaming uses the constants `PITCH_LENGTH = 105`, `PITCH_WIDTH = 68` (`consumer_utils.py:100-101`).
   For a regulation match these coincide, so this is latent rather than active.

### 3.3 Possession code → team id

Batch bronze rewrites the column *in place*:

```python
ball_possession = when(ball_possession == 1, home_team_id)
                   .when(ball_possession == 2, guest_team_id)
```

`home_team_id` / `guest_team_id` come from `match_info` (the match XML), so this generalises to any
match.

Streaming keeps the raw integer in silver (`schemas.py:74`, `IntegerType`) and resolves it *only
inside the enrichment* via `consumer_utils.POSSESSION_TEAM_MAP` (`consumer_utils.py:36`), a
hand-maintained dict covering **7 matches** — and the comments in that dict show it was filled in
partly by guessing (the `J03WMX` slot-1 entry is annotated "the one slot in the map that the data
does not back up"). **Not aligned.**

### 3.4 Metadata passthrough

Batch bronze joins `match_info` for `pitch_x`, `pitch_y`, `home_team_id`, `guest_team_id`, then
drops them. Streaming has no equivalent join in this consumer.

**BRONZE verdict:** nothing aligns because nothing exists. The batch's approach
(derive everything from data, keyed by `game_section`) is strictly more general than the streaming
approach (hard-code the direction and the half boundary for one match). Closing this gap is the
single highest-value change in the whole comparison.

---

## 4. SILVER level

### 4.1 The enrichment algorithm — **ALIGNED**

`batch/silver/silver_enrichment.py::enrich_and_pair_batch` and
`scripts/consumer_positions_pairs.py::enrich_and_pair_batch` (`line 604`) are the *same algorithm*,
both batch-numpy, and agree on every point checked:

| Step | Both implementations |
|---|---|
| Grouping | by `(match_id, frame_id)`, rows collected in batch order |
| Ball lookup | `flatnonzero(team_ids == "BALL")[0]` within the group |
| `ball_distance` | `sqrt((x-bx)² + (y-by)²)` for **every** row in the group (players, ball, referee); NaN where coordinates are non-finite |
| `has_possession` | candidates = rows whose `team_id` equals the possession team, finite and present; the single **nearest to the ball** is flagged `True` |
| Pairing teams | first two teams in order of first appearance (`seen` dict — note the streaming comment `was pd.unique`) |
| Pairing matrix | full `n×m` distance matrix, mutual-nearest filter: keep only pairs where `pair_dist == row_min == col_min` |
| `closest_opponent_distance` | `max(row_min, col_min)` — the larger of the two one-sided minima |
| `target_distance` | `sqrt((x-52.5)² + (y-34.0)²)`, NaN where non-finite |
| Exclusions | `BALL` and `referee` are excluded from pairing (but **not** from `ball_distance`) |
| `_is_missing_scalar` | identical `None`/NaN semantics |

`TARGET_POINT = (52.5, 34.0)` in both (`silver_enrichment.py:10` and `consumer_utils.py:83`).

So the *arithmetic* is aligned. The problem is the *inputs*.

### 4.2 Coordinate system — **NOT ALIGNED (most important finding)**

| | Batch silver | Streaming pairs silver |
|---|---|---|
| Coordinates used | `x_norm`, `y_norm` | `coordinate_columns()` → `"X", "Y"` (raw, un-normalised) |
| Orientation | attack-normalised, +x = attacking, 0…105 | raw provider frame, no direction handling |
| Half-time | handled in bronze | **not handled at all** |

`consumer_positions_pairs.py` contains **no occurrence** of `x_norm`, `y_norm`, `attack_direction`,
`pitch_x` or `pitch_y` — confirmed by grep across `consumer_positions_pairs.py`, `producer.py`
and `schemas.py`.

Consequences, all of them silent (no error, just a different number):

- `target_distance` — distance to the centre circle, computed in a frame where that point is
  `(52.5, 34.0)` only for one team in one half.
- `ball_distance`, `pair_player_distance`, `closest_opponent_distance` — **these are invariant** to
  a rigid translation/reflection, so they are numerically fine, but they are computed in mirrored
  axes, so "which side" is not recoverable downstream.
- Anything that aggregates across teams within a match is inconsistent: team A's distances are in
  one frame, team B's in a mirrored one.

Note the webui compensates for the missing normalisation *in the renderer* (`PITCH_FLIP_Y`, and the
second-half `attackDirs` negation), not in the data. That is a presentation-layer fix; the silver
rows themselves remain un-normalised.

### 4.3 Schema — **NOT ALIGNED**

Streaming (`scripts/schemas.py:61`, `SILVER_POSITIONS_SCHEMA`), 20 columns:

```
source_timestamp:Long, batch_id:Long, match_id:String, game_section:String, frame_id:Long,
team_id:String, player_id:String, X:Double, Y:Double, D:Double, S:Double, A:Double,
ball_possession:Int, ball_status:Int, ball_distance:Double, has_possession:Boolean,
pair_player_id:String, pair_player_distance:Double, closest_opponent_distance:Double,
target_distance:Double
```

Batch (`schemas/silver_positions_schema.py`), 19 columns:

```
match_id:String, game_section:String, frame_id:Long, team_id:String, person_id:String,
x_norm:Double, y_norm:Double, distance:Double, speed:Double, acceleration:Double,
ball_possession:String, ball_status:Int, ball_distance:Double, has_possession:Boolean,
pair_player_id:String, pair_player_distance:Double, closest_opponent_distance:Double,
target_distance:Double
```

| Concept | Batch | Streaming | Note |
|---|---|---|---|
| Player id | `person_id` | `player_id` | rename only |
| Coordinates | `x_norm`, `y_norm` | `X`, `Y` | **different coordinate system**, see §4.2 |
| Kinematics | `distance`, `speed`, `acceleration` | `D`, `S`, `A` | rename only (D is *cumulative* distance, S speed, A acceleration) |
| `ball_possession` | `String` (team id) | `Integer` (1/2 code) | **type and meaning both differ** |
| Lineage | — | `source_timestamp`, `batch_id` | batch has no lineage; streaming's timestamp is Kafka arrival, not match time |
| Match clock | — | — | **neither** silver has it (batch drops `timestamp` that raw had) |

### 4.4 Rounding — **NOT ALIGNED**

Batch rounds every numeric at the output: `F.round(..., 3)` in `silver_enrichment.py:256-267`
(and `2` decimals for `x_norm`/`y_norm` in bronze).

Streaming performs **no rounding anywhere** — grep for `round`/`ROUND` in
`consumer_positions_pairs.py` returns only comments. Full `float64` precision is published to
Kafka.

Effect: batch and streaming silver rows for the same frame differ in the last digits even where the
formulas match. Not a correctness problem, but it means the two cannot be compared with `==` and
join/dedup across the two pipelines would produce spurious duplicates.

### 4.5 Row filtering — **minor difference**

Both keep `referee` and `BALL` rows in the enrichment output (batch filters them only later, in
`silver_positions_grouped`). **Aligned at this level.**

Batch's enrichment input filter (`silver_enrichment.py:245-252`) requires
`match_id`, `frame_id`, `team_id` non-null and `person_id` non-null *unless* the row is
`BALL`/`referee`; streaming applies the equivalent `*_present` masks. **Aligned.**

**SILVER verdict:** the maths is aligned; the coordinate system, the schema and the rounding are not.

---

## 5. GOLD level — **absent from streaming**

`consumer_positions_pairs.py` emits no gold stage. The batch has two:

1. `silver_positions_grouped` (materialized view, `silver/silver_positions.py`)
2. `gold_possessions` (materialized view, `gold/gold_possessions.py`)

### 5.1 Grouped view

| Feature | Batch | Notes |
|---|---|---|
| Referee filtered | yes | |
| Row grain | one row per `(match_id, team_id, frame_id, game_section)`, players in a `players` struct array | |
| Array order | `array_sort(players, by x_norm ascending)` | |
| `offside_line` | `get(players, **1**).x_norm` — the **second**-deepest player | see below |
| `play_state` | `ball_status == 1` → `"active"`, else `"interruption"` | |

### 5.2 Possessions

| Feature | Batch |
|---|---|
| `has_possession` (team level) | possessing team id == `team_id`, via a frame window on the `BALL` group's `ball_possession` |
| Opponent line | `max(offside_line)` over non-possessing team in the frame |
| Sequence id | `row_num - row_num_possession` over `(match_id, game_section)` — a stateless "difference of row numbers" trick |
| `possession_id` | `row_number()` over `(match_id, game_section)` ordered by `start_frame` |
| `duration_sec` | `num_frames / 25` (`FPS = 25`) |
| `cumulative_time` | running sum of `duration_sec`, formatted `MM:SS` |
| Aggregates | `num_frames`, `active_frames`, `interruption_frames`, `avg_ball_speed`, `avg_offside_line`, `opponent_avg_offside_line`, `opponent_id` |
| Output | nested `team_metrics` / `opponent_metrics` structs |

### 5.3 Issues inside the batch gold itself

These are worth flagging because they will surface the moment this code is ported:

- **`offside_line` is off by one player.** The array is sorted ascending by `x_norm`, so index `0`
  is the deepest attacker and index `1` is the second-deepest. The batch takes index `1`. Everywhere
  else in the codebase the deepest player is meant (`consumer_positions_pipeline.py:607` uses
  `min(x_norm)`, and the batch's own `gold` uses `get(players, 0)` for the ball). `get(players, 1)`
  looks like a leftover.
- **`avg_ball_speed`** is computed as
  `avg(when(get(players,0).distance > 0, get(players,0).speed))`. `players[0]` is the *deepest
  outfield player*, not the ball — the ball lives in the separate `BALL` team group. So this field
  is mislabelled; it is an outfield player's speed conditioned on that player's cumulative
  distance being positive (always true). `consumer_positions_pipeline.py:896` has the same
  expression but uses `ball_positions.*`, which *is* the ball — so this is a batch-only bug.
- **`play_state` uses `ball_status`** while the streaming equivalent uses `ball.distance > 0`. These
  are different signals (one is an event-state flag, the other "is the ball being tracked"), and the
  two pipelines also disagree on the literal: batch emits `"active"`, streaming emits `"active_play"`.
  Any cross-pipeline comparison of `active_frames` will be wrong on both axes.
- **`cumulative_time` cannot exceed 90:00 safely** — it is `MM:SS` with no hour field, and it sums
  `duration_sec` over `(match_id, game_section)`, so a second-half possession list restarts at
  00:00 (which is intentional) but a full first half cannot be represented past 90 minutes.

**GOLD verdict:** nothing exists in `consumer_positions_pairs.py`. Functionally equivalent logic
does exist in `consumer_positions_pipeline.py` (§7), with several material differences.

---

## 6. Level-by-level table of aligned / not aligned

| # | Item | Level | Status |
|---|---|---|---|
| 1 | Same source JSONL, same explode | RAW | aligned |
| 2 | `frame_id = n`, `FrameSet` extraction | RAW | aligned |
| 3 | `D`/`S`/`A`/`X`/`Y` typed `double` | RAW | aligned |
| 4 | `Frame.T` → timestamp | RAW | **batch only** (streaming projects it away) |
| 5 | `M`, `Z` columns | RAW | **batch only** (unused downstream either way) |
| 6 | One row per entity per frame | RAW | aligned |
| 7 | Attack direction from data, keyed by `game_section` | BRONZE | **batch only** |
| 8 | Direction + half boundary hard-coded for 1 match | BRONZE | **streaming only** (wrong consumer) |
| 9 | `x_norm` / `y_norm` | BRONZE/SILVER | **batch only** for the pairs consumer |
| 10 | `y` negated for `dir == 1` | BRONZE | **batch only**, and *opposite* to streaming pipeline |
| 11 | Pitch dims from match XML vs constants | BRONZE | **batch derives**, streaming hard-codes 105/68 |
| 12 | `ball_possession` 1/2 → team id, in the data | BRONZE | **batch only** (streaming resolves at use time) |
| 13 | Possession map coverage 7 matches vs any match | BRONZE | batch is general, streaming is a fixed dict |
| 14 | `ball_distance` computation | SILVER | aligned |
| 15 | `has_possession` = nearest possessing player | SILVER | aligned |
| 16 | Mutual-nearest-neighbour pairing | SILVER | aligned |
| 17 | `closest_opponent_distance` = `max(row_min, col_min)` | SILVER | aligned |
| 18 | `target_distance` to `(52.5, 34.0)` | SILVER | formula aligned, **coordinate frame not aligned** |
| 19 | Referee/BALL excluded from pairing, kept in distances | SILVER | aligned |
| 20 | Column names / types | SILVER | **not aligned** (see §4.3) |
| 21 | 3-decimal output rounding | SILVER | **batch only** |
| 22 | Lineage columns | SILVER | **streaming only** (and the timestamp is arrival time) |
| 23 | Grouped silver view | GOLD | **batch only** |
| 24 | `offside_line` definition | GOLD | differs (batch: 2nd-deepest; pipeline: min over non-GK) |
| 25 | `play_state` derivation + literal | GOLD | differs (`ball_status==1`/"active" vs `distance>0`/"active_play") |
| 26 | Possession sequencing (`group_id` trick vs change flag) | GOLD | both exist, different mechanisms |
| 27 | `duration_sec`, `cumulative_time`, `possession_id` | GOLD | aligned in intent, different implementation |
| 28 | `avg_ball_speed` source row | GOLD | **batch bug** — outfield player, not the ball |
| 29 | `FPS = 25` | GOLD | aligned (matches the data) |

---

## 7. What the batch already implements in *other* streaming consumers

Per the request, this section looks outside `consumer_positions_pairs.py`.

### 7.1 `consumer_positions_pipeline.py` — a near-complete streaming equivalent of the batch

This consumer already implements **almost the entire batch pipeline**, stage for stage, and writes
to `…-silver-positions`, `…-gold-positions`, `…-gold-possession-zones` and `…-gold-possessions-all`.

| Batch feature | Implemented in `consumer_positions_pipeline.py` | Notes |
|---|---|---|
| `attack_direction` | line 250-296 | hard-coded map, **not** derived |
| `x_norm` / `y_norm` | line 297-310 | same algebra, **`y` not negated** — differs from batch |
| `half-time flip` | via `HALF_START_FRAME` in both BALL and player branches | hard-coded |
| `ball_distance` | line 515-523 | `round(...,2)` |
| `prev_ball_distance` / lagged values | line 524+ | batch has **no** lagged fields |
| `ball_pitch_zone` / `pitch_zone_expr` | line 403-412 (`first-third`/`second-third`, `left`/`centre`/`right`) | batch has no zone columns |
| grouped per-team view | line 563-584 `collect_list(struct(...))` | batch equivalent |
| `offside_line` | line 607 `round(min(when(~is_goalkeeper, x_norm)),2)` | **min** (deepest) — differs from batch's index 1 |
| `offside_line_perc`, `team_avg_height`, `team_height_span(_perc)`, `team_avg_width` | line 609-628 | **batch has none of these** — streaming is richer |
| `play_state` | line 643 `when(ball.distance > 0, "active_play")` | **different signal and literal** from batch |
| `possession_zone` + mirrored zone for the opponent | line 647+ | **batch has none** |
| `has_possession` (team level) | line 631-640 from `ball_possession` 1/2 | matches batch's gold logic |
| GK flag | `is_goalkeeper` with `_FALLBACK_GOALKEEPERS` in `consumer_utils.py` | batch has none |
| possession sequencing, `possession_id`, `duration_sec`, `duration_min`, `cumulative_time` | line 888-921 | same output shape as batch gold |
| `team_metrics` / `opponent_metrics` structs | line 915-923 | same shape as batch gold |

**Conclusion:** the batch's bronze, grouped-silver and gold logic is *already* implemented in
streaming — but in `consumer_positions_pipeline.py`, not in `consumer_positions_pairs.py`. The batch
is therefore **not a net-new feature**; it is a second implementation of existing ground, differing
in four substantive ways (direction derivation, `y` sign, offside-line definition, play-state
signal).

### 7.2 Other consumers

| Consumer | Batch-equivalent content |
|---|---|
| `consumer_gold_join.py` | `offside_line`, `group_id`, `…-gold-positions-penalty-area`. Consumes silver, not raw. |
| `consumer_positions_lag.py` | `x_norm`/`y_norm`, `offside_line`, `play_state`; writes `…-bronze-positions` and `…-gold-positions`. A third partial implementation of the same stages. |
| `consumer_positions_pipeline_stateful.py` | Stateful variant of `consumer_positions_pipeline.py`; additionally writes `…-gold-stateful-v`. Same calculations, plus state carried across micro-batches (no lagged-field recomputation). |
| `consumer_spark_kafka.py` / `consumer_spark_s3_v1.py` / `consumer_spark.py` | Ingestion/mirror jobs (`…-events-raw`, `…-match-information`, `…-positions-raw-observed`). **No** batch-equivalent calculation. |
| `consumer_positions_pairs.py` | Mutual-nearest pairing only. **No** bronze, **no** grouped silver, **no** gold. |
| `consumer_utils.py` | The shared hard-coded maps (`POSSESSION_TEAM_MAP` ×7, `ATTACKING_DIRECTION` ×1, `HALF_START_FRAME` ×1, `PITCH_LENGTH/WIDTH`, `TARGET_POINT`, GK fallbacks, offside/height thresholds). This is the file that would have to change to make streaming data-derived. |

### 7.3 Batch features with **no** streaming equivalent anywhere

- `raw.match_info` → `pitch_x` / `pitch_y` / `home_team_id` / `guest_team_id` as a **data join**.
  Streaming has the metadata topic (`…-match-information`) but every consumer that needs this
  hard-codes it instead.
- `attack_direction` **derived from the data** (any match, any half, no constants).
- The `y`-negated normalisation (`-y + pitch_y/2`), i.e. true attack-relative *lateral* mirroring.
- 3-decimal deterministic rounding on the silver output.

---

## 8. Recommendations (no code changed)

Ordered by value per unit of risk.

1. **Decide the canonical coordinate system before anything else.** Batch's `y_norm` and
   streaming's `y_norm` disagree in sign for one team. Pick one, then either (a) make
   `consumer_positions_pipeline.py` negate `y` to match the batch, or (b) drop the negation from the
   batch and accept a single global convention. Doing this first avoids re-deriving every downstream
   metric.
2. **Move `attack_direction` and `HALF_START_FRAME` from constants to a derivation.**
   `ATTACKING_DIRECTION` covers 1 match, `HALF_START_FRAME` covers 1 match, `POSSESSION_TEAM_MAP`
   covers 7. The batch's `game_section`-keyed approach is the general one. A hybrid — derive from
   the kickoff frame of each `game_section`, fall back to the hard-coded value when the match is
   unknown — is the low-risk version and immediately unblocks matches 2-7.
3. **Reconcile `offside_line`.** Decide deepest (`min`) or second-deepest, and make the batch's
   `get(players, 1)` match. This one index changes every gold possession aggregate.
4. **Reconcile `play_state`.** `ball_status == 1` vs `ball.distance > 0`, and the literal
   `"active"` vs `"active_play"`. These are genuinely different signals; picking one changes
   `active_frames` / `interruption_frames` and hence every possession summary.
5. **Fix the batch's `avg_ball_speed`** (it reads an outfield player, not the ball).
6. **Add `Frame.T` back to the streaming raw projection.** It is the only way to get a real match
   clock into silver; the webui currently has to reverse-engineer it.
7. **Decide on rounding.** Batch rounds to 3 dp, streaming does not round. Pick one so the two
   pipelines can be diffed and cross-joined.
8. **Do not run both.** `consumer_positions_pipeline.py` and the batch already cover the same
   ground. The useful question is not "which is better" but "which becomes the single source of
   truth", with the other becoming a validation job rather than a parallel production.
9. **If `consumer_positions_pairs.py` is to keep its current scope**, document it explicitly as
   "raw → flat enriched silver only, un-normalised, for the physics/pairs visualisation", and stop
   treating it as a stage-equivalent of the batch. Its value is the mutual-nearest pairing, which
   the batch has verbatim and the other consumers do not.

---

## Appendix: files read

```
Batch (from pipeline_databricks_batch.zip)
  raw/raw_positions.py                     35 lines
  raw/match_info.py                        22
  bronze/bronze_positions.py               94
  silver/silver_positions.py               45
  silver/silver_enrichment.py             270
  schemas/silver_positions_schema.py       34
  gold/gold_possessions.py                 70

Streaming
  scripts/consumer_positions_pairs.py    1292
  scripts/consumer_positions_pipeline.py  976
  scripts/consumer_utils.py
  scripts/schemas.py                       189
  scripts/producer.py
```