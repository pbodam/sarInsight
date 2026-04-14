import pandas as pd

pandas = pd  # ensure 'pandas' name is available
from sar_parser import run_sar, convert_time

# --- sar -P ALL (per-CPU utilization; not a single trailing-float block) ---
_CPU_COLUMNS = ["time", "cpu", "user", "nice", "system", "iowait", "steal", "idle"]


def get_cpu_data(file, source_tz="UTC", target_tz="UTC"):
    data = run_sar("sar", "-f", file, "-P", "ALL")

    rows = []
    for r in data:
        if len(r) >= len(_CPU_COLUMNS):
            try:
                float(r[2])
                float(r[4])
                float(r[5])
                rows.append(r[: len(_CPU_COLUMNS)])
            except (ValueError, TypeError):
                continue
    df = pd.DataFrame(rows, columns=_CPU_COLUMNS)

    df["user"] = pd.to_numeric(df["user"], errors="coerce")
    df["system"] = pd.to_numeric(df["system"], errors="coerce")
    df["iowait"] = pd.to_numeric(df["iowait"], errors="coerce")
    df["steal"] = pd.to_numeric(df["steal"], errors="coerce")
    df["idle"] = pd.to_numeric(df["idle"], errors="coerce")
    df["time"] = convert_time(df["time"], source_tz, target_tz)

    return df


# --- sar -q: run queue + load averages (generic tabular) ---
LOAD_QUEUE_COLUMNS = ("runq_sz", "plist_sz", "ldavg_1", "ldavg_5", "ldavg_15")


def _sar_tabular_to_df(file, sar_args, metric_columns, source_tz="UTC", target_tz="UTC"):
    """Parse generic sar tabular output into a DataFrame.

    Supports both 24h rows:
      HH:MM:SS <metric_1> ...
    and 12h rows:
      HH:MM:SS AM|PM <metric_1> ...
    """
    data = run_sar("sar", *sar_args, "-f", file)
    rows = []
    expected = len(metric_columns)

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

    columns = ["time"] + list(metric_columns)
    if not rows:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(rows, columns=columns)
    for col in metric_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["time"] = convert_time(df["time"], source_tz, target_tz)
    return df


def _sar_tabular_to_df_try_variants(
    file, sar_args, metric_variants, source_tz="UTC", target_tz="UTC"
):
    """Try multiple metric layouts and return the first successful parse."""
    for metrics in metric_variants:
        df = _sar_tabular_to_df(file, sar_args, metrics, source_tz, target_tz)
        if not df.empty:
            return df
    # Preserve expected shape for downstream checks even when empty.
    return pd.DataFrame(columns=["time"] + list(metric_variants[0]))


def get_load_queue_data(file, source_tz="UTC", target_tz="UTC"):
    return _sar_tabular_to_df(file, ("-q",), LOAD_QUEUE_COLUMNS, source_tz, target_tz)


# --- sar -w: process creation + context switches (generic tabular; column count varies by sysstat) ---
_CONTEXT_SWITCH_VARIANTS = (
    ("proc_s", "cswch_s"),
    ("cswch_s",),
)


def get_context_switch_data(file, source_tz="UTC", target_tz="UTC"):
    return _sar_tabular_to_df_try_variants(
        file, ("-w",), _CONTEXT_SWITCH_VARIANTS, source_tz, target_tz
    )
