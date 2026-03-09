import pandas as pd

pandas = pd  # ensure 'pandas' name is available
from sar_parser import run_sar, convert_time


def get_disk_data(file, source_tz="UTC", target_tz="UTC"):
    """Parse sar -d output. Supports 9-col (rkB/s,wkB/s,await,util) or 7-col (rd_sec,wr_sec,await,util)."""
    data = run_sar("sar", "-d", "-f", file)

    columns_9 = ["time", "device", "tps", "rkB_s", "wkB_s", "areq_sz", "aqu_sz", "await", "util"]
    columns_7 = ["time", "device", "tps", "rd_sec", "wr_sec", "await", "util"]
    rows = []
    for r in data:
        # AM/PM format: "12:00:01 AM dev8-0 ..." -> time+AM at r[0:2], device at r[2]
        if len(r) >= 10 and r[1] in ("AM", "PM"):
            try:
                float(r[8])
                float(r[9])
                rows.append([r[0] + " " + r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]])
            except (ValueError, TypeError):
                continue
        elif len(r) >= 8 and r[1] in ("AM", "PM"):
            try:
                float(r[6])
                float(r[7])
                rows.append([r[0] + " " + r[1], r[2], r[3], r[4], r[5], r[6], r[7]])
            except (ValueError, TypeError):
                continue
        elif len(r) >= 9:
            try:
                float(r[7])
                float(r[8])
                rows.append(r[:9])
            except (ValueError, TypeError):
                continue
        elif len(r) >= 7:
            try:
                float(r[5])
                float(r[6])
                rows.append(r[:7])
            except (ValueError, TypeError):
                continue

    columns = columns_9 if rows and len(rows[0]) == 9 else columns_7
    df = pd.DataFrame(rows, columns=columns)

    if "areq_sz" in df.columns:
        df["areq_sz"] = pd.to_numeric(df["areq_sz"], errors="coerce")
    if "aqu_sz" in df.columns:
        df["aqu_sz"] = pd.to_numeric(df["aqu_sz"], errors="coerce")
    df["await"] = pd.to_numeric(df["await"], errors="coerce")
    df["util"] = pd.to_numeric(df["util"], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df