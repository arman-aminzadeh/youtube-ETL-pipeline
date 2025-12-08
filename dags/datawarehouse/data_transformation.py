
from datetime import timedelta
import re

from datetime import timedelta
import re

def parse_duration(duration_str):
    if not duration_str:
        return timedelta(0)

    pattern = r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?"
    match = re.match(pattern, duration_str)
    if not match:
        return timedelta(0)

    hours   = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    return timedelta(hours=hours, minutes=minutes, seconds=seconds)

def transform_date(row):
    duration_td = parse_duration(row.get("duration"))

    # store as "HH:MM:SS"
    total_seconds = int(duration_td.total_seconds())
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    row["Duration"] = f"{h:02}:{m:02}:{s:02}"

    row["Video_Type"] = "Shorts" if total_seconds <= 60 else "Normal"
    return row
