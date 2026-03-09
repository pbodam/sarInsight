"""Parse sar -n DEV output (network device statistics)."""
import pandas as pd

from sar_parser import run_sar, convert_time


def get_network_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -n DEV output.
    Columns: time, iface, rxpck_s, txpck_s, rxkB_s, txkB_s, rxcmp_s, txcmp_s, rxmcst_s, ifutil
    """
    data = run_sar("sar", "-n", "DEV", "-f", file)

    columns = ["time", "iface", "rxpck_s", "txpck_s", "rxkB_s", "txkB_s", "rxcmp_s", "txcmp_s", "rxmcst_s", "ifutil"]
    rows = []
    for r in data:
        if len(r) >= len(columns):
            if r[1] == "IFACE":
                continue
            try:
                float(r[2])  # rxpck_s must be numeric
                float(r[3])  # txpck_s
                rows.append(r[: len(columns)])
            except (ValueError, TypeError):
                continue
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)

    for col in ["rxpck_s", "txpck_s", "rxkB_s", "txkB_s", "rxcmp_s", "txcmp_s", "rxmcst_s", "ifutil"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df
