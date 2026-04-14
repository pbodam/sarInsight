"""
Swap page I/O: ``sar -W`` (pswpin/s, pswpout/s).
``sar -S`` is swap space in kB, not page rates.
"""
import pandas as pd

from sar_parser import convert_time, run_sar

SWAP_IO_COLUMNS = ("pswpin_s", "pswpout_s")


def get_swap_io_data(file, source_tz="UTC", target_tz="UTC"):
    data = run_sar("sar", "-W", "-f", file)
    rows = []
    expected = len(SWAP_IO_COLUMNS)

    for r in data:
        if len(r) >= expected + 2 and r[1] in ("AM", "PM"):
            ts = f"{r[0]} {r[1]}"
            values = r[2 : 2 + expected]
        elif len(r) >= expected + 1:
            ts = r[0]
            values = r[1 : 1 + expected]
        else:
            continue

        try:
            [float(v) for v in values]
            rows.append([ts] + values)
        except (ValueError, TypeError):
            continue

    columns = ["time"] + list(SWAP_IO_COLUMNS)
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)
    for col in SWAP_IO_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["time"] = convert_time(df["time"], source_tz, target_tz)
    return df
