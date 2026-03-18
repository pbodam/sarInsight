import pandas as pd

pandas = pd  # ensure 'pandas' name is available
from sar_parser import run_sar, convert_time


def get_memory_data(file, source_tz="UTC", target_tz="UTC"):

    data = run_sar("sar", "-r", "-f", file)

    columns = ["time", "kbmemfree", "kbmemused", "kbbuffers", "kbcached", "pctmemused"]
    rows = []
    for r in data:
        # Format: time, kbmemfree, kbavail, kbmemused, %memused, kbbuffers, kbcached, ...
        if len(r) >= 12:
            row = [r[0], r[1], r[3], r[5], r[6], r[4]]
        # Format: "12:10:02 AM" (7 parts) - time splits into [time, AM/PM, kbmemfree, kbmemused, ...]
        elif len(r) == 7 and r[1] in ("AM", "PM"):
            row = [r[0] + " " + r[1], r[2], r[3], r[5], r[6], r[4]]
        elif len(r) >= 6:
            row = [r[0], r[1], r[2], r[4], r[5], None]
        else:
            continue
        try:
            float(row[2])  # kbmemused must be numeric
            rows.append(row)
        except (ValueError, TypeError):
            continue
    df = pd.DataFrame(rows, columns=columns)

    df["kbmemused"] = pd.to_numeric(df["kbmemused"], errors="coerce")
    df["kbmemfree"] = pd.to_numeric(df["kbmemfree"], errors="coerce")
    df["kbbuffers"] = pd.to_numeric(df["kbbuffers"], errors="coerce")
    df["kbcached"] = pd.to_numeric(df["kbcached"], errors="coerce")
    df["pctmemused"] = pd.to_numeric(df["pctmemused"], errors="coerce")

    total_mem_kb = df["kbmemused"] + df["kbmemfree"]
    safe_total = total_mem_kb.where(total_mem_kb > 0)

    # Return memory values as percentages instead of absolute units.
    df["kbmemused"] = df["pctmemused"].where(df["pctmemused"].notna(), (df["kbmemused"] / safe_total) * 100)
    df["kbmemfree"] = 100 - df["kbmemused"]
    df["kbbuffers"] = (df["kbbuffers"] / safe_total) * 100
    df["kbcached"] = (df["kbcached"] / safe_total) * 100
    df = df.drop(columns=["pctmemused"])

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df