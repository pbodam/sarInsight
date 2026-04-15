import argparse
import os
from typing import Dict, Optional

import pandas as pd

from cpu_module import get_cpu_data, get_context_switch_data, get_load_queue_data
from disk_module import get_disk_data
from memory_module import get_memory_data
from memory_swap_in_out import get_swap_io_data
from network_module import get_network_data
from network_edev_module import get_network_edev_data
from sar_plot_utils import coerce_time_column
from sar_parser import get_hostname
from socket_module import get_socket_data


def _safe_stats(series: pd.Series) -> Dict[str, Optional[float]]:
    if series is None or series.empty:
        return {"mean": None, "max": None, "min": None}
    return {
        "mean": float(series.mean()),
        "max": float(series.max()),
        "min": float(series.min()),
    }


def _fmt(value: Optional[float], suffix: str = "", precision: int = 1) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{value:.{precision}f}{suffix}"


def _top_n(df: pd.DataFrame, group_col: str, metric: str, n: int = 2) -> str:
    if df is None or df.empty or group_col not in df.columns or metric not in df.columns:
        return ""
    top = df.groupby(group_col)[metric].max().nlargest(n)
    return ", ".join(f"{idx} ({val:.1f})" for idx, val in top.items())


def load_frames(path: str, tz: str = "UTC") -> Dict[str, pd.DataFrame]:
    return {
        "cpu": coerce_time_column(get_cpu_data(path, "UTC", tz)),
        "load_queue": coerce_time_column(get_load_queue_data(path, "UTC", tz)),
        "context": coerce_time_column(get_context_switch_data(path, "UTC", tz)),
        "memory": coerce_time_column(get_memory_data(path, "UTC", tz)),
        "swap_io": coerce_time_column(get_swap_io_data(path, "UTC", tz)),
        "disk": coerce_time_column(get_disk_data(path, "UTC", tz)),
        "network": coerce_time_column(get_network_data(path, "UTC", tz)),
        "network_errors": coerce_time_column(get_network_edev_data(path, "UTC", tz)),
        "sockets": coerce_time_column(get_socket_data(path, "UTC", tz)),
    }


def _find_anomalies(df: pd.DataFrame, col: str, threshold: float, direction: str = "above") -> str:
    if df is None or df.empty or col not in df.columns:
        return ""
    if direction == "above":
        anomalies = df[df[col] > threshold]
    elif direction == "below":
        anomalies = df[df[col] < threshold]
    else:
        return ""
    if anomalies.empty:
        return ""
    max_row = anomalies.loc[anomalies[col].idxmax()]
    time = max_row["time"]
    value = max_row[col]
    return f" (peak {value:.1f} at {time})"


def summarize_cpu(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "CPU: no data available."

    if "total" not in df.columns:
        if all(c in df.columns for c in ("user", "system", "iowait", "steal")):
            df = df.assign(total=df["user"] + df["system"] + df["iowait"] + df["steal"])

    total_stats = _safe_stats(df["total"]) if "total" in df.columns else None
    idle_stats = _safe_stats(df["idle"]) if "idle" in df.columns else None
    high_peak = total_stats and total_stats["max"] is not None and total_stats["max"] > 90
    high_avg = total_stats and total_stats["mean"] is not None and total_stats["mean"] > 75
    summary = ["CPU:"]
    if total_stats:
        summary.append(
            f" average usage { _fmt(total_stats['mean'], '%') }, peak { _fmt(total_stats['max'], '%') }."
        )
    if idle_stats:
        summary.append(f" average idle { _fmt(idle_stats['mean'], '%') }." )
    anomaly = _find_anomalies(df, "total", 90) if "total" in df.columns else ""
    if anomaly:
        summary.append(f" High usage spike{anomaly}.")
    elif high_peak and high_avg:
        summary.append("This workload shows sustained high CPU usage.")
    elif high_peak:
        summary.append("There are intermittent CPU spikes.")
    else:
        summary.append("CPU utilization appears moderate.")
    return "".join(summary)


def summarize_load_queue(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Load average: no data available."

    stats = {c: _safe_stats(df[c]) for c in ("ldavg_1", "ldavg_5", "ldavg_15") if c in df.columns}
    if not stats:
        return "Load average: no valid metrics found."

    line = "Load average:"
    line += " " + ", ".join(
        f"{name[6:]}min avg {_fmt(value['mean'])}, max {_fmt(value['max'])}" for name, value in stats.items()
    )
    peak = stats.get("ldavg_1", {}).get("max")
    anomaly = _find_anomalies(df, "ldavg_1", 2.0) if "ldavg_1" in df.columns else ""
    if anomaly:
        line += f" High load spike{anomaly}."
    elif peak is not None and peak > 2.0:
        line += " High load spikes are present."
    elif peak is not None and peak > 1.0:
        line += " Load is above a single core on average."
    else:
        line += " Load remains generally light."
    return line


def summarize_context_switch(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Context switches: no data available."
    parts = []
    for col in ("proc_s", "cswch_s"):
        if col in df.columns:
            stats = _safe_stats(df[col])
            parts.append(f"{col} avg {_fmt(stats['mean'])}, peak {_fmt(stats['max'])}")
    if not parts:
        return "Context switches: no valid metrics found."
    anomaly = _find_anomalies(df, "cswch_s", 10000) if "cswch_s" in df.columns else ""
    conclusion = "Context switches: " + "; ".join(parts) + "."
    if anomaly:
        conclusion += f" High context switching{anomaly}."
    return conclusion


def summarize_memory(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Memory: no data available."

    metrics = ["kbmemused", "kbmemfree", "kbbuffers", "kbcached"]
    parts = []
    for col in metrics:
        if col in df.columns:
            stats = _safe_stats(df[col])
            parts.append(f"{col} avg {_fmt(stats['mean'])}%")
    if not parts:
        return "Memory: no valid metrics found."
    used_avg = _safe_stats(df["kbmemused"]) if "kbmemused" in df.columns else None
    conclusion = "Memory usage is stable."
    anomaly = _find_anomalies(df, "kbmemused", 80) if "kbmemused" in df.columns else ""
    if anomaly:
        conclusion = f"High memory usage detected{anomaly}."
    elif used_avg and used_avg["mean"] is not None and used_avg["mean"] > 80:
        conclusion = "Memory usage is high."
    return "Memory: " + ", ".join(parts) + ". " + conclusion


def summarize_swap(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Swap I/O: no data available."
    stats_in = _safe_stats(df["pswpin_s"]) if "pswpin_s" in df.columns else None
    stats_out = _safe_stats(df["pswpout_s"]) if "pswpout_s" in df.columns else None
    if not stats_in and not stats_out:
        return "Swap I/O: no valid metrics found."
    if (stats_in and stats_in["max"] and stats_in["max"] > 0) or (stats_out and stats_out["max"] and stats_out["max"] > 0):
        verdict = "Swap activity was observed."
        anomaly_in = _find_anomalies(df, "pswpin_s", 0) if "pswpin_s" in df.columns else ""
        anomaly_out = _find_anomalies(df, "pswpout_s", 0) if "pswpout_s" in df.columns else ""
        if anomaly_in or anomaly_out:
            verdict += f" Peak swap in{anomaly_in}. Peak swap out{anomaly_out}."
    else:
        verdict = "Minimal or no swap activity was observed."
    parts = []
    if stats_in:
        parts.append(f"in avg {_fmt(stats_in['mean'])}, peak {_fmt(stats_in['max'])}")
    if stats_out:
        parts.append(f"out avg {_fmt(stats_out['mean'])}, peak {_fmt(stats_out['max'])}")
    return f"Swap I/O: {', '.join(parts)}. {verdict}"


def summarize_disk(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Disk: no data available."
    parts = []
    if "await" in df.columns:
        stats = _safe_stats(df["await"])
        parts.append(f"await avg {_fmt(stats['mean'], ' ms')}, peak {_fmt(stats['max'], ' ms')}")
    if "util" in df.columns:
        stats = _safe_stats(df["util"])
        parts.append(f"util avg {_fmt(stats['mean'], '%')}, peak {_fmt(stats['max'], '%')}")
    if not parts:
        return "Disk: no valid metrics found."
    conclusion = "Disk performance appears normal."
    anomaly_util = _find_anomalies(df, "util", 85) if "util" in df.columns else ""
    anomaly_await = _find_anomalies(df, "await", 20) if "await" in df.columns else ""
    if anomaly_util:
        conclusion = f"High disk utilization{anomaly_util}."
    elif anomaly_await:
        conclusion = f"High disk latency{anomaly_await}."
    elif "util" in df.columns and df["util"].max() > 85:
        conclusion = "Disk utilization is high at times."
    elif "await" in df.columns and df["await"].max() > 20:
        conclusion = "Disk latency is elevated at times."
    top_devices = _top_n(df, "device", "util", 2)
    if top_devices:
        conclusion += f" Top devices by utilization: {top_devices}."
    return "Disk: " + ", ".join(parts) + ". " + conclusion


def summarize_network(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Network: no data available."
    parts = []
    if "rxkB_s" in df.columns:
        stats = _safe_stats(df["rxkB_s"])
        parts.append(f"rx avg {_fmt(stats['mean'], ' kB/s')}, peak {_fmt(stats['max'], ' kB/s')}")
    if "txkB_s" in df.columns:
        stats = _safe_stats(df["txkB_s"])
        parts.append(f"tx avg {_fmt(stats['mean'], ' kB/s')}, peak {_fmt(stats['max'], ' kB/s')}")
    if "ifutil" in df.columns:
        stats = _safe_stats(df["ifutil"])
        parts.append(f"util avg {_fmt(stats['mean'], '%')}, peak {_fmt(stats['max'], '%')}")
    if not parts:
        return "Network: no valid metrics found."
    conclusion = "Network throughput is stable."
    anomaly = _find_anomalies(df, "ifutil", 80) if "ifutil" in df.columns else ""
    if anomaly:
        conclusion = f"High network utilization{anomaly}."
    elif "ifutil" in df.columns and df["ifutil"].max() > 80:
        conclusion = "Some interfaces show high utilization."
    top_ifaces = _top_n(df, "iface", "txkB_s", 2)
    if not top_ifaces:
        top_ifaces = _top_n(df, "iface", "rxkB_s", 2)
    if top_ifaces:
        conclusion += f" Top interfaces: {top_ifaces}."
    return "Network: " + ", ".join(parts) + ". " + conclusion


def summarize_network_errors(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Network errors: no data available."
    metrics = [
        "rxerr_s",
        "txerr_s",
        "coll_s",
        "rxdrop_s",
        "txdrop_s",
        "txcarr_s",
        "rxfram_s",
        "rxfifo_s",
        "txfifo_s",
    ]
    totals = {}
    for col in metrics:
        if col in df.columns:
            total = float(df[col].sum(skipna=True))
            if total > 0:
                totals[col] = total
    if not totals:
        return "Network errors: no error activity detected."
    summary = ", ".join(f"{col} total {int(total)}" for col, total in totals.items())
    return "Network errors: " + summary + ". Significant error spikes detected."


def summarize_sockets(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "Sockets: no data available."
    parts = []
    for col in ("totsck", "tcpsck", "udpsck", "tcp_tw"):
        if col in df.columns:
            stats = _safe_stats(df[col])
            parts.append(f"{col} avg {_fmt(stats['mean'])}, peak {_fmt(stats['max'])}")
    if not parts:
        return "Sockets: no valid metrics found."
    anomaly = _find_anomalies(df, "totsck", 1000) if "totsck" in df.columns else ""
    conclusion = "Sockets: " + "; ".join(parts) + "."
    if anomaly:
        conclusion += f" High socket count{anomaly}."
    return conclusion


def generate_summary(path: str, tz: str = "UTC") -> str:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"SAR file not found: {path}")

    summary = []
    try:
        data = load_frames(path, tz)
    except Exception as exc:
        raise RuntimeError(f"Failed to load SAR data: {exc}") from exc

    hostname = get_hostname(path) or "unknown host"
    summary.append(f"SAR summary for {os.path.basename(path)} on {hostname}")
    summary.append("-")
    summary.append(summarize_cpu(data["cpu"]))
    summary.append(summarize_load_queue(data["load_queue"]))
    summary.append(summarize_context_switch(data["context"]))
    summary.append(summarize_memory(data["memory"]))
    summary.append(summarize_swap(data["swap_io"]))
    summary.append(summarize_disk(data["disk"]))
    summary.append(summarize_network(data["network"]))
    summary.append(summarize_network_errors(data["network_errors"]))
    summary.append(summarize_sockets(data["sockets"]))
    return "\n".join(summary)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a text summary from a SAR report.")
    parser.add_argument("file", help="Path to the SAR binary report file")
    parser.add_argument("--timezone", default="UTC", help="Target timezone for times in the report (default: UTC)")
    args = parser.parse_args()

    summary = generate_summary(args.file, args.timezone)
    print(summary)


if __name__ == "__main__":
    main()
