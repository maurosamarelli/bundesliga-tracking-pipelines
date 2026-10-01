import xml.etree.ElementTree as ET


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

# ------------------------------------------------------------
# Static attacking direction per match (derived at Frame N=10000,
# first half, from each team's mean X position):
#
#   +1 = attacks sx -> dx (rightward, own goal on the left half)
#   -1 = attacks dx -> sx (leftward,  own goal on the right half)
#
# Convention used to normalize a frame:
#   attacking direction +1 -> x_norm = x + pitch / 2
#   attacking direction -1 -> x_norm = pitch - (x + pitch / 2)
# ------------------------------------------------------------
attacking_direction_map = {
   "DFL-MAT-J03WMX": {
      "DFL-CLU-000008": -1,
      "DFL-CLU-00000G": 1,
   },
   "DFL-MAT-J03WN1": {
      "DFL-CLU-00000B": 1,
      "DFL-CLU-00000S": -1,
   },
   "DFL-MAT-J03WOH": {
      "DFL-CLU-00000P": -1,
      "DFL-CLU-000011": 1,
   },
   "DFL-MAT-J03WOY": {
      "DFL-CLU-00000P": -1,
      "DFL-CLU-00000Q": 1,
   },
   "DFL-MAT-J03WPY": {
      "DFL-CLU-000005": 1,
      "DFL-CLU-00000P": -1,
   },
   "DFL-MAT-J03WQQ": {
      "DFL-CLU-00000H": 1,
      "DFL-CLU-00000P": -1,
   },
   "DFL-MAT-J03WR9": {
      "DFL-CLU-00000I": -1,
      "DFL-CLU-00000P": 1,
   },
}

# ------------------------------------------------------------
# Shared matchinformation XML parsing (used by producer.py and the
# webui match catalog so both stay in sync).
# ------------------------------------------------------------
def parse_team_element(team_el):
    players = [dict(p.attrib) for p in team_el.find("Players").iter("Player")]
    trainers = [dict(t.attrib) for t in team_el.find("TrainerStaff").iter("Trainer")]
    team = dict(team_el.attrib)
    team["Players"] = players
    team["TrainerStaff"] = trainers
    return team


def parse_match_information_xml(xml_bytes):
    """Parse a matchinformation XML body into the same document dict used
    by the producer and the web UI. Returns None if it is not a valid
    match-information document (no MatchId)."""
    root = ET.fromstring(xml_bytes)
    match_information = root.find("MatchInformation")
    if match_information is None:
        return None
    document = {
        "General": dict(match_information.find("General").attrib),
        "Environment": dict(match_information.find("Environment").attrib),
        "Teams": [parse_team_element(t) for t in match_information.find("Teams").iter("Team")],
        "Referees": [dict(r.attrib) for r in match_information.find("Referees").iter("Referee")],
        "OtherGameInformation": dict(match_information.find("OtherGameInformation").attrib),
    }
    if not document["General"].get("MatchId"):
        return None
    return document
