"""Parse sar -n SOCK output (socket statistics, system-wide)."""
import subprocess

import pandas as pd

from sar_parser import run_sar, convert_time

# Linux sar SOCK: totsck tcpsck udpsck rawsck ip-frag tcp-tw
SOCKET_COLUMNS = ["time", "totsck", "tcpsck", "udpsck", "rawsck", "ip_frag", "tcp_tw"]


def get_socket_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -n SOCK output (no per-interface breakdown).
    """
    try:
        data = run_sar("sar", "-n", "SOCK", "-f", file)
    except (subprocess.CalledProcessError, OSError, FileNotFoundError):
        return pd.DataFrame(columns=SOCKET_COLUMNS)

    rows = []
    for r in data:
        if "SOCK" in r and len(r) <= 3:
            continue
        if len(r) >= 8 and r[1] in ("AM", "PM"):
            try:
                float(r[2])
                rows.append([r[0] + " " + r[1]] + r[2:8])
            except (ValueError, TypeError, IndexError):
                continue
        elif len(r) >= 7:
            try:
                float(r[1])
                rows.append([r[0]] + r[1:7])
            except (ValueError, TypeError, IndexError):
                continue

    if not rows:
        return pd.DataFrame(columns=SOCKET_COLUMNS)

    df = pd.DataFrame(rows, columns=SOCKET_COLUMNS)
    for col in SOCKET_COLUMNS[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["time"] = convert_time(df["time"], source_tz, target_tz)
    return df
