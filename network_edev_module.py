"""Parse sar -n EDEV output (network device errors)."""
import pandas as pd

from sar_parser import run_sar, convert_time


def get_network_edev_data(file, source_tz="UTC", target_tz="UTC"):
    """
    Parse sar -n EDEV output.
    Columns: time, iface, rxerr_s, txerr_s, coll_s, rxdrop_s, txdrop_s, txcarr_s, rxfram_s, rxfifo_s, txfifo_s
    """
    data = run_sar("sar", "-n", "EDEV", "-f", file)

    columns = [
        "time", "iface",
        "rxerr_s", "txerr_s", "coll_s", "rxdrop_s", "txdrop_s",
        "txcarr_s", "rxfram_s", "rxfifo_s", "txfifo_s",
    ]
    rows = []
    for r in data:
        if len(r) >= 11:
            if len(r) > 1 and r[1] == "IFACE":
                continue
            try:
                float(r[2])  # rxerr_s
                float(r[3])  # txerr_s
                rows.append(r[: len(columns)])
            except (ValueError, TypeError):
                continue
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)

    for col in ["rxerr_s", "txerr_s", "coll_s", "rxdrop_s", "txdrop_s", "txcarr_s", "rxfram_s", "rxfifo_s", "txfifo_s"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df
