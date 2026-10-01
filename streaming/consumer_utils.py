import os

import boto3
from pyspark.sql import functions as F

from utils import parse_match_information_xml

S3_REGION = os.environ.get("S3_REGION", "eu-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET", "bundesliga-2022-2023-data")

# Map match_id -> {ball_possession_value: team_id}
# ball_possession is a numeric slot from the raw tracking data (1 or 2).
#
# A match that is MISSING from this map gets no possession at all: enrich_
# and_pair_frame calls possession_team_for(), which returns None for an
# unknown match_id, so has_possession stays False for every player. Downstream
# that means no ball glyph in the frame-analysis Pairs list and no green
# possession ring in the Pressing pane. Every match that gets streamed needs
# an entry here.
#
# Provenance of each entry below (home/guest taken from the match-information
# XML the pipeline already parses):
#
#   [verified]  slot mapping confirmed against the silver stream, by checking
#               which club's player is nearest the ball on frames carrying that
#               ball_possession value.
#   [stream]    the match has stream data and the mapping agrees with the
#               home/guest ordering, but at least one slot was under ~70%
#               confident, so treat it as provisional.
#   [xml only]  the match has never been streamed, so the mapping rests on the
#               home/guest ordering alone. Re-check it the first time it runs.
#
# Measured confidences are in the comments. The "nearest player to the ball" test
# is a proxy: during a challenge or a loose ball the other side can be briefly
# closer, so anything under ~70% is noise rather than a counter-example.
POSSESSION_TEAM_MAP = {
    "DFL-MAT-J03WMX": {
        # 1. FC Köln vs FC Bayern München.
        # Slot 2 confirmed (guest nearest on 88% of 650 frames). Slot 1 only
        # had 119 frames and came out 57% toward the GUEST, i.e. against the
        # home/guest ordering — too weak to overturn the original entry, but
        # this is the one slot in the map that the data does not back up.
        1: "DFL-CLU-000008",  # 1. FC Köln          (home  → slot 1)  [stream]
        2: "DFL-CLU-00000G",  # FC Bayern München   (guest → slot 2)  [verified]
    },
    "DFL-MAT-J03WN1": {
        # VfL Bochum 1848 vs Bayer 04 Leverkusen. Never streamed.
        1: "DFL-CLU-00000S",  # VfL Bochum 1848        (home  → slot 1)  [xml only]
        2: "DFL-CLU-00000B",  # Bayer 04 Leverkusen    (guest → slot 2)  [xml only]
    },
    "DFL-MAT-J03WOH": {
        # Fortuna Düsseldorf vs SSV Jahn Regensburg.
        # Slot 1 home on 76% of 33 frames; slot 2 guest on 66% of 837.
        1: "DFL-CLU-00000P",   # Fortuna Düsseldorf    (home  → slot 1)  [stream]
        2: "DFL-CLU-000011",   # SSV Jahn Regensburg   (guest → slot 2)  [stream]
    },
    "DFL-MAT-J03WOY": {
        # Fortuna Düsseldorf vs F.C. Hansa Rostock.
        # Slot 1 home on 87% of 611 frames; slot 2 guest on 54% of 259.
        1: "DFL-CLU-00000P",   # Fortuna Düsseldorf    (home  → slot 1)  [verified]
        2: "DFL-CLU-00000Q",   # F.C. Hansa Rostock    (guest → slot 2)  [stream]
    },
    "DFL-MAT-J03WPY": {
        # Fortuna Düsseldorf vs 1. FC Nürnberg. Never streamed.
        1: "DFL-CLU-00000P",   # Fortuna Düsseldorf    (home  → slot 1)  [xml only]
        2: "DFL-CLU-000005",   # 1. FC Nürnberg        (guest → slot 2)  [xml only]
    },
    "DFL-MAT-J03WQQ": {
        # Fortuna Düsseldorf vs FC St. Pauli. Never streamed.
        1: "DFL-CLU-00000P",   # Fortuna Düsseldorf    (home  → slot 1)  [xml only]
        2: "DFL-CLU-00000H",   # FC St. Pauli          (guest → slot 2)  [xml only]
    },
    "DFL-MAT-J03WR9": {
        # Fortuna Düsseldorf vs 1. FC Kaiserslautern.
        # Slot 1 home on 88% of 674 frames; slot 2 guest on 74% of 196.
        1: "DFL-CLU-00000P",   # Fortuna Düsseldorf    (home  → slot 1)  [verified]
        2: "DFL-CLU-00000I",   # 1. FC Kaiserslautern  (guest → slot 2)  [verified]
    },
}

# Static target point for distance calculations (X, Y)
# Example: (52.5, 34.0) is roughly the center of a 105x68 pitch
TARGET_POINT = (52.5, 34.0)

partition_team_map = {
   "referee" : 0,
   "BALL" : 1,
   "DFL-CLU-00000G" : 2,
   "DFL-CLU-000008" : 3,
   "DFL-CLU-00000B" : 4,
   "DFL-CLU-00000S" : 5,
   "DFL-CLU-00000P" : 6,
   "DFL-CLU-000011" : 7,
   "DFL-CLU-00000Q" : 8,
   "DFL-CLU-000005" : 9,
   "DFL-CLU-00000H" : 10,
   "DFL-CLU-00000I" : 11 
}

PITCH_LENGTH = 105
PITCH_WIDTH = 68
PENALTY_AREA = {
   "x_min" : 88.5,
   "x_max" : 105,
   "y_min" : 13.84,
   "y_max" : 54.16
}
CLOSE_BALL_DISTANCE = 2
APPROACH_SCORE = 0.1

ATTACKING_DIRECTION = F.create_map(
    F.lit("DFL-MAT-J03WMX"), 
    F.create_map(
        F.lit("DFL-CLU-00000G"), F.lit(1),
        F.lit("DFL-CLU-000008"), F.lit(-1)
    )
)

#Threshold for kpi that defines team state
THRESHOLD_OFFSIDE_LINE_PERC = 33.33
THRESHOLD_HEIGHT_SPAN_PERC = 50

# Frame id where the SECOND half starts, per match. Teams swap ends at
# half-time, so the direction above (derived in the 1st half at Frame
# N=10000) must flip for frames at/after this boundary, otherwise x_norm /
# y_norm (and everything built on them: silver team metrics, gold penalty
# area) are mirrored for the whole 2nd half.
#
# Verified for J03WMX on both the raw XML and the streamed timemajor JSONL:
#   last  firstHalf frame : N=80707  T=14:17:20.880Z
#   first secondHalf frame: N=100000 T=14:35:43.360Z  (matches the
#   GameSection="secondHalf" KickOff event at 14:35:43Z)
# Frame ids are NOT reset at half-time; N=10000 is the first frame of the
# whole match (13:30:12Z), not the second half.
HALF_START_FRAME = F.create_map(
    F.lit("DFL-MAT-J03WMX"), F.lit(100000),
)

# A frame_id jump larger than this between consecutive frames of the same
# (match_id, player_id) marks a data gap (e.g. the half-time break). The
# lagged prev_* fields are reset (nulled) for the first frame after such
# a gap instead of keeping 20-minutes-stale values.
FRAME_GAP_RESET_THRESHOLD = 1000

_FALLBACK_GOALKEEPERS = {
    "DFL-CLU-00000G": "DFL-OBJ-0002DR",   # FC Bayern München
    "DFL-CLU-000008": "DFL-OBJ-0002HE",   # 1. FC Köln
}


def load_goalkeepers_from_matchinfo():
    """team_id -> starting goalkeeper PersonId for every team in the S3
    matchinformation catalog (PlayingPosition == "TW"), derived from data
    instead of a hand-written list so every match consumes correctly."""
    goalkeepers = {}
    s3 = boto3.client("s3", region_name=S3_REGION)

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix="matchinformation/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".xml") and "_02_01_matchinformation_" in key:
                keys.append(key)

    for key in sorted(keys):
        try:
            document = parse_match_information_xml(
                s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
            )
        except Exception:
            continue
        if not document:
            continue

        for team in document.get("Teams") or []:
            team_id = team.get("TeamId")
            if not team_id or team_id in goalkeepers:
                continue

            gk_person = None
            for player in team.get("Players") or []:
                if (player.get("PlayingPosition") or "").upper() != "TW":
                    continue
                starts = str(player.get("Starting") or "").lower() in ("true", "1", "yes")
                if gk_person is None or starts:
                    gk_person = player.get("PersonId")

            if gk_person:
                goalkeepers[team_id] = gk_person

    return goalkeepers


try:
    goalkeeper_data = {**_FALLBACK_GOALKEEPERS, **load_goalkeepers_from_matchinfo()}
except Exception:
    goalkeeper_data = dict(_FALLBACK_GOALKEEPERS)

GOALKEEPERS = F.create_map(*[
    F.lit(value)
    for item in goalkeeper_data.items()
    for value in item
])

