"""Parse sar -q output (run queue and process/load statistics)."""
import pandas as pd

from sar_parser import run_sar, convert_time


def get_total_process_count_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -q output.
    Columns: time, runq_sz, plist_sz, ldavg_1, ldavg_5, ldavg_15, blocked
    """
    data = run_sar("sar", "-q", "-f", file)

    columns = ["time", "runq_sz", "plist_sz", "ldavg_1", "ldavg_5", "ldavg_15", "blocked"]
    rows = []

    for r in data:
        row = None
        # 24-hour format, example:
        # 08:50:01 4 1126 1.86 2.19 2.47 0
        if len(r) >= 7:
            row = [r[0], r[1], r[2], r[3], r[4], r[5], r[6]]
        # AM/PM format, example:
        # 08:50:01 AM 4 1126 1.86 2.19 2.47 0
        if len(r) >= 8 and r[1] in ("AM", "PM"):
            row = [r[0] + " " + r[1], r[2], r[3], r[4], r[5], r[6], r[7]]

        if row is None:
            continue

        try:
            float(row[1])  # runq_sz
            float(row[2])  # plist_sz
            rows.append(row)
        except (ValueError, TypeError):
            continue

    df = pd.DataFrame(rows, columns=columns)
    if df.empty:
        return df

    numeric_cols = ["runq_sz", "plist_sz", "ldavg_1", "ldavg_5", "ldavg_15", "blocked"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)
    return df
