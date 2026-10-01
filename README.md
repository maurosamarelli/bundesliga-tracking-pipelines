# Bundesliga 2022-23 tracking pipelines

Two implementations of the **same four-stage football tracking pipeline**, kept
side by side on purpose:

| | |
|---|---|
| `batch/` | The original **Databricks** pipeline (`@dp.table` / `@dp.materialized_view`), reading time-major JSON from S3 and computing the whole match at once. |
| `streaming/` | **`consumer_positions_pairs_v2.py`** — the same four stages as PySpark Structured Streaming, on Kafka, micro-batch by micro-batch. |

`consumer_positions_pairs_v2.py` is a faithful streaming port of the batch, not a
rewrite. Its enrichment is verified **bit-identical** to the batch's own
implementation (see [Tests](#tests)), and it reproduces the batch's column names,
types, normalisation formulas and rounding.

`docs/batch-vs-streaming-alignment.md` is the level-by-level analysis of where
the two agree and where they cannot.

---

## The four stages

```
raw        raw_positions / position_rows        one row per entity per frame
bronze     bronze_positions                     attack direction, x_norm/y_norm, possession -> team id
silver     silver_enrichment / silver_positions  ball distance, possession, pairing, target distance
grouped    silver_positions_grouped              players[] per team, offside_line, play_state
gold       gold_possessions                      possession sequences and aggregates
```

---

## Batch

Databricks Declarative Pipelines. `batch/` must be uploaded as a workspace folder
so the `transformations.schemas.silver_positions_schema` import resolves — that
is why the schema lives at `batch/transformations/schemas/`, not `batch/schemas/`.

```python
@dp.table(name="`bundesliga-2022-2023`.batch.raw_positions", ...)
@dp.materialized_view(name="`bundesliga-2022-2023`.batch.bronze_positions", ...)
```

Tables: `raw_positions`, `match_info`, `bronze_positions`,
`silver_positions_enrichment`, `silver_positions_grouped`, `gold_possessions`.

Needs Databricks Runtime with PySpark, NumPy and Pandas. Tune the enrichment
shuffle with the `enrich_partitions` Spark config (default 200).

---

## Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  consumer_positions_pairs_v2.py
```

Reads `bundesliga-2022-2023-raw-positions`, writes four levels:

| Level | Topic |
|---|---|
| bronze | `bundesliga-2022-2023-bronze-positions` |
| silver | `bundesliga-2022-2023-silver-positions` |
| grouped | `bundesliga-2022-2023-silver-positions-grouped` |
| gold | `bundesliga-2022-2023-gold-possessions` |

### Writes are off by default

`V2_WRITE_ENABLED=0` runs the whole pipeline, plans and executes all four levels,
counts every row, and publishes nothing. Turn it on deliberately:

```bash
V2_WRITE_ENABLED=1 spark-submit ... consumer_positions_pairs_v2.py
```

### Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `V2_WRITE_ENABLED` | `0` | Master publish switch. `0` = dry run |
| `V2_LEVELS` | `bronze,silver,grouped,gold` | Levels to build |
| `V2_PUBLISH_LEVELS` | all | Levels to publish |
| `V2_OFFSIDE_INDEX` | `1` | Batch parity is `1` (second-deepest). `0` = deepest |
| `V2_STRICT_MATCHES` | `0` | `1` = fail instead of warning on an unknown match |
| `V2_AUDIT` | `1` | Per-batch audit of unresolved attack directions |
| `V2_WRITE_PACED` | `1` | Paced background writer vs one burst |
| `V2_ENRICH_PARTITIONS` | `8` | Enrichment shuffle partitions |
| `V2_WRITER_MIN_WINDOW` | `2.0` | Floor on the publish window, seconds |
| `V2_CHECKPOINT_DIR` | `/tmp/checkpoint-bundesliga-2022-2023-v2` | Structured Streaming checkpoint |
| `V2_KAFKA_BOOTSTRAP` | `localhost:9092` | Broker list |

### Match metadata

`ball_possession` is stored as a **team id**, which needs home/guest. The batch
reads them from the match XML; nothing produces that here, so v2 resolves them
from, in order:

1. `streaming/.match-info/<match_id>.json` — drop a file in, no code change:
   ```json
   {"home_team_id": "DFL-CLU-000008", "guest_team_id": "DFL-CLU-00000G",
    "pitch_x": 105.0, "pitch_y": 68.0}
   ```
2. `POSSESSION_TEAM_MAP` in `consumer_utils.py`
3. `PITCH_LENGTH` / `PITCH_WIDTH` constants

An **unknown match is not silent**: it is named in a warning, `ball_possession`
becomes NULL and the BALL row loses its attack direction. `V2_STRICT_MATCHES=1`
turns that into a failure.

---

## Where the two deliberately differ

**Attack direction.** The batch derives it from frame ids `10000` and `100000` —
one kickoff frame per half, keyed by `game_section`, which is what makes the
second-half flip automatic. A 30-second micro-batch contains neither frame, so v2
seeds the same map from the **earliest frame of each `(match_id, game_section)`
present in that batch**. Same idea, same per-section seeding, no state carried
between batches.

**Gold possession sequences are batch-scoped.** The batch sees the whole match, so
one sequence is one possession. In streaming a possession spanning a micro-batch
boundary is cut in two and `possession_id` / `cumulative_time` restart. Every gold
row carries `stream_batch_id` and `sequence_clipped` (true when the sequence
touches either edge of the batch's frame range) so a consumer can tell a clipped
sequence from a genuine possession change. **This is the one place where the two
cannot be made to agree.**

**Coordinates.** v2 follows the batch: `y` is negated for `attack_direction == 1`,
giving a true attack-relative lateral mirroring. Some other streaming consumers in
the same project do not negate `y`, so the two `y_norm` conventions in that project
are mirror images for one team. See the alignment doc.

---

## Tests

```bash
python3 tests/test_enrichment_parity.py
```

Executes the batch's own `enrich_and_pair_batch` (with the Databricks runtime
stubbed out) and v2's port on byte-identical frames, then compares every output
column with `rtol=0, atol=0`:

```
  [single frame]            IDENTICAL (24 rows)
  [3 frames]                IDENTICAL (72 rows)
  [two matches, two halves] IDENTICAL (96 rows)
  [degenerate]              IDENTICAL (13 rows)
  [7 frames in chunks of 3] IDENTICAL (168 rows)

PASS - the streaming enricher is bit-identical to the batch's.
```

---

## Requirements

Python 3.12, PySpark 4.x, Pandas, NumPy, `kafka-python`. The batch additionally
needs Databricks Runtime; the streaming consumer needs `boto3` transitively
(via `consumer_utils.py`, used only for helper functions v2 does not call).