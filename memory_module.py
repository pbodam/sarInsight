import pandas as pd

pandas = pd  # ensure 'pandas' name is available
from sar_parser import run_sar, convert_time


def get_memory_data(file, source_tz="UTC", target_tz="UTC"):

    data = run_sar("sar", "-r", "-f", file)

    columns = ["time", "kbmemfree", "kbmemused", "kbbuffers", "kbcached"]
    rows = []
    for r in data:
        # Format: time, kbmemfree, kbavail, kbmemused, %memused, kbbuffers, kbcached, ...
        if len(r) >= 12:
            row = [r[0], r[1], r[3], r[5], r[6]]  # time, kbmemfree, kbmemused, kbbuffers, kbcached
        # Format: "12:10:02 AM" (7 parts) - time splits into [time, AM/PM, kbmemfree, kbmemused, ...]
        elif len(r) == 7 and r[1] in ("AM", "PM"):
            row = [r[0] + " " + r[1], r[2], r[3], r[5], r[6]]
        elif len(r) >= 6:
            row = [r[0], r[1], r[2], r[4], r[5]]
        else:
            continue
        try:
            float(row[2])  # kbmemused must be numeric
            rows.append(row)
        except (ValueError, TypeError):
            continue
    df = pd.DataFrame(rows, columns=columns)

    df["kbmemused"] = pd.to_numeric(df["kbmemused"], errors="coerce") / 1024  # convert to MB
    df["kbmemfree"] = pd.to_numeric(df["kbmemfree"], errors="coerce") / 1024
    df["kbbuffers"] = pd.to_numeric(df["kbbuffers"], errors="coerce") / 1024
    df["kbcached"] = pd.to_numeric(df["kbcached"], errors="coerce") / 1024

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df