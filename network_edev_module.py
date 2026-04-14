"""Parse sar -n EDEV output (network device errors)."""
import subprocess

import pandas as pd

from sar_parser import run_sar, convert_time


def get_network_edev_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -n EDEV output.
    Columns: time, iface, rxerr_s, txerr_s, coll_s, rxdrop_s, txdrop_s, txcarr_s, rxfram_s, rxfifo_s, txfifo_s
    """
    columns = [
        "time", "iface",
        "rxerr_s", "txerr_s", "coll_s", "rxdrop_s", "txdrop_s",
        "txcarr_s", "rxfram_s", "rxfifo_s", "txfifo_s",
    ]
    try:
        data = run_sar("sar", "-n", "EDEV", "-f", file)
    except (subprocess.CalledProcessError, OSError, FileNotFoundError):
        return pd.DataFrame(columns=columns)

    # Each data row: 24h → [time, iface, 9 metrics] (11 tokens).
    # 12h → [time, AM|PM, iface, 9 metrics] (12 tokens).
    rows = []
    for r in data:
        if len(r) > 1 and r[1] == "IFACE":
            continue
        if len(r) >= 3 and r[1] in ("AM", "PM"):
            if len(r) < 12 or r[2] == "IFACE":
                continue
            try:
                float(r[3])
                float(r[4])
                row = [r[0] + " " + r[1], r[2]] + list(r[3:12])
                rows.append(row)
            except (ValueError, TypeError, IndexError):
                continue
        elif len(r) >= 11:
            try:
                float(r[2])
                float(r[3])
                rows.append(r[:11])
            except (ValueError, TypeError):
                continue
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)

    for col in ["rxerr_s", "txerr_s", "coll_s", "rxdrop_s", "txdrop_s", "txcarr_s", "rxfram_s", "rxfifo_s", "txfifo_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df
