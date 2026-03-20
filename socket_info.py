"""Parse sar -n SOCK output (socket statistics)."""
import pandas as pd

from sar_parser import run_sar, convert_time


def get_socket_info_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -n SOCK output.
    Columns: time, totsck, tcpsck, udpsck, rawsck, ip_frag, tcp_tw
    """
    data = run_sar("sar", "-n", "SOCK", "-f", file)

    columns = ["time", "totsck", "tcpsck", "udpsck", "rawsck", "ip_frag", "tcp_tw"]
    rows = []
    for r in data:
        # AM/PM format: HH:MM:SS AM value...
        if len(r) >= 8 and r[1] in ("AM", "PM"):
            row = [r[0] + " " + r[1], r[2], r[3], r[4], r[5], r[6], r[7]]
        elif len(r) >= 7:
            row = r[:7]
        else:
            continue
        try:
            float(row[1])  # totsck must be numeric
            rows.append(row)
        except (ValueError, TypeError):
            continue

    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)
    for col in ["totsck", "tcpsck", "udpsck", "rawsck", "ip_frag", "tcp_tw"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)
    return df
