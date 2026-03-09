import pandas as pd

pandas = pd  # ensure 'pandas' name is available
from sar_parser import run_sar, convert_time


def get_cpu_data(file, source_tz="UTC", target_tz="UTC"):

    data = run_sar("sar", "-f", file, "-P", "ALL")

    columns = ["time", "cpu", "user", "nice", "system", "iowait", "steal", "idle"]
    rows = []
    for r in data:
        if len(r) >= len(columns):
            try:
                float(r[2])
                float(r[4])
                float(r[5])
                rows.append(r[: len(columns)])
            except (ValueError, TypeError):
                continue
    df = pd.DataFrame(rows, columns=columns)

    df["user"] = pd.to_numeric(df["user"], errors="coerce")
    df["system"] = pd.to_numeric(df["system"], errors="coerce")
    df["iowait"] = pd.to_numeric(df["iowait"], errors="coerce")
    df["steal"] = pd.to_numeric(df["steal"], errors="coerce")
    df["idle"] = pd.to_numeric(df["idle"], errors="coerce")
    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df