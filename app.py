import os
import re
import secrets
import sys

import pandas as pd
from flask import Flask, redirect, render_template, request, session, url_for
from werkzeug.utils import secure_filename

# Ensure local modules are importable even when launched from another cwd.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from cpu_module import get_context_switch_data, get_cpu_data, get_load_queue_data
from memory_module import get_memory_data
from memory_swap_in_out import get_swap_io_data
from disk_module import get_disk_data
from network_module import get_network_data
from network_edev_module import get_network_edev_data
from socket_module import get_socket_data
from sar_parser import get_hostname
from sar_plot_utils import (
    DEFAULT_INITIAL_WINDOW_SEC,
    coerce_time_column,
    finalize_sar_figure_html,
    initial_x_range_from_series,
    pick_anchor_time_series,
)

import plotly.express as px
import plotly.graph_objects as go

SA_FOLDER = os.path.join(BASE_DIR, "sa")
UPLOAD_FOLDER = os.path.join(BASE_DIR, "_sar_uploads")
# Sar binaries can be large; adjust if needed (bytes).
MAX_SAR_UPLOAD_BYTES = 256 * 1024 * 1024

app = Flask(__name__)
app.config["SA_FOLDER"] = SA_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_SAR_UPLOAD_BYTES
app.secret_key = os.environ.get("SARINSIGHT_SECRET_KEY") or "sarinsight-dev-secret-change-for-production"


def _ensure_sa_folder():
    os.makedirs(SA_FOLDER, exist_ok=True)


def _get_sa_files():
    _ensure_sa_folder()
    try:
        return [f for f in os.listdir(SA_FOLDER) if os.path.isfile(os.path.join(SA_FOLDER, f))]
    except OSError:
        return []


def _ensure_upload_folder():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def _remove_path_quiet(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except OSError:
        pass


def _resolve_sar_path_for_request():
    """
    Returns (absolute_path_or_none, error_message_or_none).
    Priority: new upload > session copy > file in sa/ (GET preview only uses sa path elsewhere).
    """
    _ensure_upload_folder()
    up = request.files.get("sar_file")
    if up and up.filename:
        safe = secure_filename(up.filename) or "sar_data"
        dest = os.path.join(UPLOAD_FOLDER, f"{secrets.token_hex(8)}_{safe}")
        up.save(dest)
        old = session.get("sar_active_path")
        if old and old != dest and str(old).startswith(UPLOAD_FOLDER):
            _remove_path_quiet(old)
        session["sar_active_path"] = dest
        session["sar_display_name"] = up.filename
        return dest, None

    p = session.get("sar_active_path")
    if p and os.path.isfile(p):
        return p, None

    return None, None


GRAPH_OPTIONS = [
    "cpu",
    "load_avg",
    "context_switches",
    "memory",
    "swap_io",
    "disk",
    "network",
    "network_errors",
    "sockets",
]

# Shown in the form, page section headings, and Plotly chart titles.
GRAPH_LABELS = {
    "cpu": "CPU utilization",
    "load_avg": "Run queue & load average",
    "context_switches": "Task / context switching",
    "memory": "Memory usage",
    "swap_io": "Memory Swap in / out",
    "disk": "Disk latency and utilization",
    "network": "Network throughput",
    "network_errors": "Network errors and drops",
    "sockets": "Socket usage",
}

CPU_METRIC_COLS = ("user", "system", "iowait", "steal", "idle", "total")
CPU_LEGEND_LABELS = {
    "user": "User time (%)",
    "system": "System time (%)",
    "iowait": "I/O wait (%)",
    "steal": "Steal (%)",
    "idle": "Idle (%)",
    "total": "Total non-idle (%)",
}

DISK_METRIC_COLS = ("areq_sz", "aqu_sz", "await", "util")
DISK_LEGEND_LABELS = {
    "areq_sz": "Average request size",
    "aqu_sz": "Average queue size",
    "await": "Average wait (ms)",
    "util": "Utilization (%)",
}

NETWORK_METRIC_COLS = (
    "rxpck_s",
    "txpck_s",
    "rxkB_s",
    "txkB_s",
    "rxcmp_s",
    "txcmp_s",
    "rxmcst_s",
    "ifutil",
)
NETWORK_LEGEND_LABELS = {
    "rxpck_s": "RX packets/s",
    "txpck_s": "TX packets/s",
    "rxkB_s": "Receive KB/s",
    "txkB_s": "Transmit KB/s",
    "rxcmp_s": "RX compressed/s",
    "txcmp_s": "TX compressed/s",
    "rxmcst_s": "RX multicast/s",
    "ifutil": "Interface utilization (%)",
}

EDEV_LEGEND_LABELS = {
    "rxerr_s": "RX errors/s",
    "txerr_s": "TX errors/s",
    "coll_s": "Collisions/s",
    "rxdrop_s": "RX drops/s",
    "txdrop_s": "TX drops/s",
    "txcarr_s": "TX carrier errors/s",
    "rxfram_s": "RX frame errors/s",
    "rxfifo_s": "RX FIFO overruns/s",
    "txfifo_s": "TX FIFO overruns/s",
}

SOCKET_LEGEND_LABELS = {
    "totsck": "Total Open Sockets",
    "tcpsck": "Active TCP Sockets",
    "udpsck": "Active UDP Sockets",
    "rawsck": "Raw IP Sockets",
    "ip_frag": "IP Fragment Buffers",
    "tcp_tw": "TCP TIME_WAIT Sockets",
}

MEM_LEGEND_LABELS = {
    "kbmemfree": "Free memory (MB)",
    "kbmemused": "Used memory (MB)",
    "kbbuffers": "Buffers (MB)",
    "kbcached": "Cached (MB)",
}

SWAP_IO_LEGEND_LABELS = {
    "pswpin_s": "Pages swapped from disk into RAM per second",
    "pswpout_s": "Pages swapped from RAM to disk per second",
}

LOAD_QUEUE_LEGEND_LABELS = {
    "runq_sz": "Run queue length",
    "plist_sz": "Process list size",
    "ldavg_1": "Load average (1 min)",
    "ldavg_5": "Load average (5 min)",
    "ldavg_15": "Load average (15 min)",
}

CONTEXT_SWITCH_LEGEND_LABELS = {
    "proc_s": "Processes created per second",
    "cswch_s": "Context switches per second",
}


def _load_sa_data(path: str, tz: str):
    """Load CPU, disk, and network frames plus unique ids for filter UI (best-effort)."""

    try:
        cpu = get_cpu_data(path, "UTC", tz)
    except Exception:
        cpu = None
    try:
        disk = get_disk_data(path, "UTC", tz)
    except Exception:
        disk = None
    try:
        net = get_network_data(path, "UTC", tz)
    except Exception:
        net = None

    cpu_list, disk_list, iface_list = [], [], []
    if cpu is not None and not cpu.empty and "cpu" in cpu.columns:
        cpu_list = sorted(cpu["cpu"].unique().tolist(), key=lambda x: (x == "all", x))
    if disk is not None and "device" in disk.columns and not disk.empty:
        disk_list = sorted(disk["device"].dropna().astype(str).unique().tolist())
    if net is not None and "iface" in net.columns and not net.empty:
        iface_list = sorted(net["iface"].dropna().astype(str).unique().tolist())

    return cpu, disk, net, cpu_list, disk_list, iface_list


def _prefetch_graph_datasets(path: str, tz: str, selected_graphs: list) -> dict:
    """Run optional ``sar`` parsers for selected graphs (one subprocess each, sequential)."""
    sg = set(selected_graphs)
    out: dict = {}
    if "load_avg" in sg:
        try:
            out["load_queue"] = get_load_queue_data(path, "UTC", tz)
        except Exception:
            out["load_queue"] = None
    if "context_switches" in sg:
        try:
            out["context"] = get_context_switch_data(path, "UTC", tz)
        except Exception:
            out["context"] = None
    if "memory" in sg:
        try:
            out["memory"] = get_memory_data(path, "UTC", tz)
        except Exception:
            out["memory"] = None
    if "swap_io" in sg:
        try:
            out["swap_io"] = get_swap_io_data(path, "UTC", tz)
        except Exception:
            out["swap_io"] = None
    if "network_errors" in sg:
        try:
            out["edev"] = get_network_edev_data(path, "UTC", tz)
        except Exception:
            out["edev"] = None
    if "sockets" in sg:
        try:
            out["socket"] = get_socket_data(path, "UTC", tz)
        except Exception:
            out["socket"] = None
    return out


def _add_cpu_dropdown(fig, cpu_list):
    """Add dropdown to CPU graph to filter by CPU. Traces are grouped by legendgroup (cpu)."""
    n = len(fig.data)
    trace_to_cpu = []
    for t in fig.data:
        c = getattr(t, "legendgroup", None) or getattr(t, "name", "")
        trace_to_cpu.append(c)
    buttons = [dict(label="All CPUs", method="restyle", args=[{"visible": [True] * n}])]
    for cpu in cpu_list:
        visible = [trace_to_cpu[i] == cpu for i in range(n)]
        buttons.append(
            dict(label=f"CPU {cpu}", method="restyle", args=[{"visible": visible}])
        )
    fig.update_layout(
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                showactive=True,
                x=1.02,
                xanchor="left",
                y=1,
                yanchor="top",
            )
        ],
        annotations=[
            dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)
        ],
    )


def _tabular_sar_line_html(df, metric_cols, title, legend_map, yaxis_title, x_range=None):
    """Plotly line chart for generic SAR tabular frames (time + numeric columns)."""
    if df is None or df.empty:
        return None
    cols = [c for c in metric_cols if c in df.columns]
    if not cols:
        return None
    df = coerce_time_column(df)
    fig = px.line(
        df,
        x="time",
        y=cols,
        title=title,
        labels={c: legend_map.get(c, c) for c in cols},
    )
    fig.update_layout(
        xaxis_title="Time",
        yaxis_title=yaxis_title,
        legend=dict(title="Series", font=dict(size=11)),
    )
    return finalize_sar_figure_html(fig, x_range)


@app.route("/", methods=["GET", "POST"])
def index():
    cpu_graph = None
    load_queue_graph = None
    context_switch_graph = None
    mem_graph = None
    swap_io_graph = None
    disk_graph = None
    net_graph = None
    net_err_graph = None
    sock_graph = None
    error_message = None
    hostname = ""
    cpu_list = []
    disk_list = []
    iface_list = []
    selected_graphs = list(GRAPH_OPTIONS)
    selected_cpus = ["all"]
    selected_disks = []
    selected_ifaces = []
    iface_filter = ""
    disk_filter = ""
    selected_cpu_metrics = []
    selected_disk_metrics = []
    selected_network_metrics = []
    timezone = "UTC"
    active_sar_name = session.get("sar_display_name")

    sa_files = sorted(_get_sa_files())

    if request.method == "GET":
        if request.args.get("clear_sar") == "1":
            old = session.pop("sar_active_path", None)
            session.pop("sar_display_name", None)
            if old and str(old).startswith(UPLOAD_FOLDER):
                _remove_path_quiet(old)
            return redirect(url_for("index"))

        preview = request.args.get("preview_file")
        if preview and preview in sa_files:
            pp = os.path.join(SA_FOLDER, preview)
            if os.path.isfile(pp):
                old = session.get("sar_active_path")
                if old and str(old).startswith(UPLOAD_FOLDER) and os.path.isfile(old):
                    _remove_path_quiet(old)
                session["sar_active_path"] = pp
                session["sar_display_name"] = preview

        path_prev = session.get("sar_active_path")
        if path_prev and os.path.isfile(path_prev):
            timezone = request.args.get("timezone", "UTC")
            active_sar_name = session.get("sar_display_name")
            try:
                hostname = get_hostname(path_prev) or "(unknown)"
            except Exception:
                hostname = ""
            try:
                _, _, _, cpu_list, disk_list, iface_list = _load_sa_data(path_prev, timezone)
            except Exception:
                cpu_list, disk_list, iface_list = [], [], []
        else:
            session.pop("sar_active_path", None)
            session.pop("sar_display_name", None)
            active_sar_name = None

    if request.method == "POST":
        tz = request.form.get("timezone", "UTC")
        timezone = tz
        selected_graphs = request.form.getlist("graphs") or list(GRAPH_OPTIONS)
        selected_cpus = request.form.getlist("cpus")
        selected_cpu_metrics = request.form.getlist("cpu_metrics")
        selected_disks = request.form.getlist("disks")
        selected_ifaces = request.form.getlist("ifaces")
        selected_disk_metrics = request.form.getlist("disk_metrics")
        selected_network_metrics = request.form.getlist("network_metrics")
        iface_filter = request.form.get("iface_filter", "") or ""
        disk_filter = request.form.get("disk_filter", "") or ""

        def _parse_filter_text(text):
            values = [v.strip() for v in re.split(r"[;,\s]+", text.strip()) if v.strip()]
            return values

        if iface_filter.strip():
            selected_ifaces = _parse_filter_text(iface_filter)
        if disk_filter.strip():
            selected_disks = _parse_filter_text(disk_filter)

        path, _upload_err = _resolve_sar_path_for_request()
        active_sar_name = session.get("sar_display_name")

        if not path:
            session.pop("sar_active_path", None)
            session.pop("sar_display_name", None)
            active_sar_name = None
            error_message = "Please choose a file, then click Analyze."

        if path and os.path.isfile(path):
            try:
                hostname = get_hostname(path) or "(unknown)"
                cpu, disk, net, cpu_list, disk_list, iface_list = _load_sa_data(path, tz)
                extra = _prefetch_graph_datasets(path, tz, selected_graphs)
                cpu = coerce_time_column(cpu)
                disk = coerce_time_column(disk)
                net = coerce_time_column(net)
                for _ek in list(extra.keys()):
                    extra[_ek] = coerce_time_column(extra[_ek])
                anchor_series = pick_anchor_time_series(cpu, disk, net, extra)
                anchor_rng = initial_x_range_from_series(
                    anchor_series, DEFAULT_INITIAL_WINDOW_SEC
                )
                if cpu is None:
                    cpu = pd.DataFrame()
                if not selected_cpus and cpu_list:
                    selected_cpus = ["all"] if "all" in cpu_list else cpu_list[:1]

                if cpu.empty or "cpu" not in cpu.columns:
                    cpu_df = cpu.copy()
                else:
                    cpu_df = (
                        cpu[cpu["cpu"].isin(selected_cpus)].copy()
                        if selected_cpus
                        else cpu.copy()
                    )
                if not cpu_df.empty and all(
                    c in cpu_df.columns for c in ("user", "system", "iowait", "steal")
                ):
                    cpu_df["total"] = (
                        cpu_df["user"]
                        + cpu_df["system"]
                        + cpu_df["iowait"]
                        + cpu_df["steal"]
                    )

                if "cpu" in selected_graphs and not cpu_df.empty:
                    name_map = CPU_LEGEND_LABELS
                    pairs = []
                    for key in selected_cpu_metrics:
                        if "_" not in key:
                            continue
                        cpu_key, metric = key.rsplit("_", 1)
                        if metric in CPU_METRIC_COLS:
                            pairs.append((cpu_key, metric))

                    cpu_graph = None
                    if pairs:
                        cpu_fig = go.Figure()
                        for cpu_key, metric in pairs:
                            sub = cpu_df[cpu_df["cpu"] == cpu_key]
                            if sub.empty:
                                continue
                            disp = name_map.get(metric, metric)
                            trace_name = f"CPU {cpu_key} · {disp}"
                            cpu_fig.add_trace(
                                go.Scatter(
                                    x=sub["time"],
                                    y=sub[metric],
                                    mode="lines",
                                    name=trace_name,
                                    legendgroup=str(cpu_key),
                                    hovertemplate=(
                                        f"<b>{trace_name}</b><br>Time: %{{x}}<br>"
                                        "Value: %{y:.2f}%<extra></extra>"
                                    ),
                                )
                            )
                        if cpu_fig.data:
                            leg = dict(title="Series", font=dict(size=10), tracegroupgap=0)
                            if len(cpu_fig.data) > 24:
                                leg["font"] = dict(size=8)
                            cpu_fig.update_layout(
                                title=GRAPH_LABELS["cpu"],
                                legend=leg,
                                yaxis=dict(title="Usage %"),
                            )
                            cpu_graph = finalize_sar_figure_html(cpu_fig, anchor_rng)

                    if cpu_graph is None:
                        if (cpu_df["cpu"] == "all").any():
                            cpu_plot = cpu_df[cpu_df["cpu"] == "all"].copy()
                        elif len(cpu_df["cpu"].unique()) == 1:
                            cpu_plot = cpu_df.copy()
                        else:
                            cpu_plot = cpu_df.groupby("time")[
                                ["user", "system", "iowait", "steal", "idle"]
                            ].mean(numeric_only=True).reset_index()
                            cpu_plot["total"] = (
                                cpu_plot["user"]
                                + cpu_plot["system"]
                                + cpu_plot["iowait"]
                                + cpu_plot["steal"]
                            )
                        cpu_fig = px.line(
                            cpu_plot,
                            x="time",
                            y=["user", "system", "iowait", "steal", "idle", "total"],
                            title=GRAPH_LABELS["cpu"],
                        )
                        for t in cpu_fig.data:
                            raw = str(t.name or "").lower()
                            t.name = "Total" if "total" in raw else name_map.get(raw, raw)
                            t.hovertemplate = (
                                f"<b>{t.name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}%<extra></extra>"
                            )
                        cpu_fig.update_layout(
                            legend=dict(title="Metric", font=dict(size=11)),
                            yaxis=dict(title="Usage %"),
                        )
                        cpu_graph = finalize_sar_figure_html(cpu_fig, anchor_rng)

                if "load_avg" in selected_graphs:
                    lq = extra.get("load_queue")
                    load_queue_graph = _tabular_sar_line_html(
                        lq,
                        (
                            "runq_sz",
                            "plist_sz",
                            "ldavg_1",
                            "ldavg_5",
                            "ldavg_15",
                        ),
                        GRAPH_LABELS["load_avg"],
                        LOAD_QUEUE_LEGEND_LABELS,
                        "Run queue / load",
                        anchor_rng,
                    )

                if "context_switches" in selected_graphs:
                    cw = extra.get("context")
                    ctx_cols = tuple(c for c in ("proc_s", "cswch_s") if c in cw.columns) if cw is not None else ()
                    context_switch_graph = _tabular_sar_line_html(
                        cw,
                        ctx_cols,
                        GRAPH_LABELS["context_switches"],
                        CONTEXT_SWITCH_LEGEND_LABELS,
                        "Per second",
                        anchor_rng,
                    )

                if "memory" in selected_graphs:
                    mem = extra.get("memory")
                    mem_cols = []
                    if mem is not None and not mem.empty:
                        mem_cols = [
                            c
                            for c in ("kbmemfree", "kbmemused", "kbbuffers", "kbcached")
                            if c in mem.columns
                        ]
                    if mem is not None and not mem.empty and mem_cols:
                        mem = coerce_time_column(mem)
                        mem_fig = px.line(mem, x="time", y=mem_cols, title=GRAPH_LABELS["memory"])
                    elif mem is not None and not mem.empty:
                        mem = coerce_time_column(mem)
                        mem_fig = px.line(mem, x="time", y="time", title=GRAPH_LABELS["memory"])
                    else:
                        mem_fig = None
                    if mem_fig is not None:
                        name_map = MEM_LEGEND_LABELS
                        for t in mem_fig.data:
                            t.name = name_map.get(str(t.name), str(t.name))
                        if mem_cols:
                            mem_fig.update_layout(yaxis_title="Megabytes (MB)")
                        mem_graph = finalize_sar_figure_html(mem_fig, anchor_rng)

                if "swap_io" in selected_graphs:
                    swap_df = extra.get("swap_io")
                    swap_io_graph = _tabular_sar_line_html(
                        swap_df,
                        ("pswpin_s", "pswpout_s"),
                        GRAPH_LABELS["swap_io"],
                        SWAP_IO_LEGEND_LABELS,
                        "Pages per second",
                        anchor_rng,
                    )

                if "disk" in selected_graphs and disk is not None and not disk.empty:
                    disk_df = disk.copy()
                    disk_df["device"] = disk_df["device"].astype(str)
                    if selected_disks:
                        disk_df = disk_df[disk_df["device"].isin(selected_disks)]
                    dnmap = DISK_LEGEND_LABELS
                    pairs_d = []
                    for key in selected_disk_metrics:
                        if "::" not in key:
                            continue
                        dev, metric = key.split("::", 1)
                        if metric in DISK_METRIC_COLS and metric in disk_df.columns:
                            pairs_d.append((dev, metric))
                    if pairs_d:
                        disk_fig = go.Figure()
                        for dev, metric in pairs_d:
                            sub = disk_df[disk_df["device"] == dev]
                            if sub.empty:
                                continue
                            nm = f"{dev} · {dnmap.get(metric, metric)}"
                            disk_fig.add_trace(
                                go.Scatter(
                                    x=sub["time"],
                                    y=sub[metric],
                                    mode="lines",
                                    name=nm,
                                    legendgroup=dev,
                                    hovertemplate=f"<b>{nm}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                )
                            )
                        if disk_fig.data:
                            disk_fig.update_layout(
                                title=GRAPH_LABELS["disk"],
                                legend=dict(title="Series", font=dict(size=10), tracegroupgap=0),
                            )
                            disk_graph = finalize_sar_figure_html(disk_fig, anchor_rng)
                    if disk_graph is None and not disk_df.empty:
                        ycols = [c for c in ("await", "util") if c in disk_df.columns]
                        if ycols:
                            disk_fig = px.line(
                                disk_df,
                                x="time",
                                y=ycols,
                                color="device",
                                title=GRAPH_LABELS["disk"],
                                labels={c: DISK_LEGEND_LABELS.get(c, c) for c in ycols},
                            )
                            disk_graph = finalize_sar_figure_html(disk_fig, anchor_rng)

                if "network" in selected_graphs and net is not None and not net.empty:
                    net_df = net.copy()
                    net_df["iface"] = net_df["iface"].astype(str)
                    if selected_ifaces:
                        net_df = net_df[net_df["iface"].isin(selected_ifaces)]
                    nnmap = NETWORK_LEGEND_LABELS
                    pairs_n = []
                    for key in selected_network_metrics:
                        if "::" not in key:
                            continue
                        iface, metric = key.split("::", 1)
                        if metric in NETWORK_METRIC_COLS and metric in net_df.columns:
                            pairs_n.append((iface, metric))
                    if pairs_n:
                        net_fig = go.Figure()
                        for iface, metric in pairs_n:
                            sub = net_df[net_df["iface"] == iface]
                            if sub.empty:
                                continue
                            disp = nnmap.get(metric, metric)
                            nm = f"{iface} · {disp}"
                            net_fig.add_trace(
                                go.Scatter(
                                    x=sub["time"],
                                    y=sub[metric],
                                    mode="lines",
                                    name=nm,
                                    legendgroup=iface,
                                    hovertemplate=f"<b>{nm}</b><br>Time: %{{x}}<br>Value: %{{y:.4g}}<extra></extra>",
                                )
                            )
                        if net_fig.data:
                            net_fig.update_layout(
                                title=GRAPH_LABELS["network"],
                                legend=dict(title="Series", font=dict(size=10), tracegroupgap=0),
                            )
                            net_graph = finalize_sar_figure_html(net_fig, anchor_rng)
                    if net_graph is None and not net_df.empty:
                        ycols = [c for c in ("rxpck_s", "txpck_s") if c in net_df.columns]
                        if ycols:
                            net_fig = px.line(
                                net_df,
                                x="time",
                                y=ycols,
                                color="iface",
                                title=GRAPH_LABELS["network"],
                                labels={c: NETWORK_LEGEND_LABELS.get(c, c) for c in ycols},
                            )
                            net_graph = finalize_sar_figure_html(net_fig, anchor_rng)

                if "network_errors" in selected_graphs:
                    edev = extra.get("edev")
                    if edev is not None and not edev.empty:
                        edev_df = edev.copy()
                        edev_df["iface"] = edev_df["iface"].astype(str)
                        if selected_ifaces:
                            edev_df = edev_df[edev_df["iface"].isin(selected_ifaces)]
                        err_cols = [
                            c
                            for c in (
                                "rxerr_s",
                                "txerr_s",
                                "coll_s",
                                "rxdrop_s",
                                "txdrop_s",
                                "txcarr_s",
                                "rxfram_s",
                                "rxfifo_s",
                                "txfifo_s",
                            )
                            if c in edev_df.columns
                        ]
                        if err_cols and not edev_df.empty:
                            melted = edev_df.melt(
                                id_vars=["time", "iface"],
                                value_vars=err_cols[:8],
                                var_name="metric",
                                value_name="value",
                            )
                            melted["metric_label"] = melted["metric"].map(
                                lambda m: EDEV_LEGEND_LABELS.get(m, m)
                            )
                            melted["series"] = (
                                melted["iface"].astype(str)
                                + " · "
                                + melted["metric_label"].astype(str)
                            )
                            net_err_fig = px.line(
                                melted,
                                x="time",
                                y="value",
                                color="series",
                                title=GRAPH_LABELS["network_errors"],
                                labels={
                                    "series": "Interface · metric",
                                    "value": "Rate (per second)",
                                    "time": "Time",
                                },
                            )
                            net_err_graph = finalize_sar_figure_html(net_err_fig, anchor_rng)

                if "sockets" in selected_graphs:
                    sock = extra.get("socket")
                    if sock is not None and not sock.empty:
                        sock_cols = [
                            c
                            for c in (
                                "totsck",
                                "tcpsck",
                                "udpsck",
                                "rawsck",
                                "ip_frag",
                                "tcp_tw",
                            )
                            if c in sock.columns
                        ]
                        if sock_cols:
                            sock = coerce_time_column(sock)
                            sock_fig = px.line(
                                sock,
                                x="time",
                                y=sock_cols,
                                title=GRAPH_LABELS["sockets"],
                                labels={
                                    c: SOCKET_LEGEND_LABELS.get(c, c) for c in sock_cols
                                },
                            )
                            sock_graph = finalize_sar_figure_html(sock_fig, anchor_rng)

            except Exception as e:
                error_message = str(e)

    return render_template(
        "index.html",
        cpu_graph=cpu_graph,
        load_queue_graph=load_queue_graph,
        context_switch_graph=context_switch_graph,
        mem_graph=mem_graph,
        swap_io_graph=swap_io_graph,
        disk_graph=disk_graph,
        net_graph=net_graph,
        net_err_graph=net_err_graph,
        sock_graph=sock_graph,
        error_message=error_message,
        hostname=hostname,
        cpu_list=cpu_list,
        disk_list=disk_list,
        iface_list=iface_list,
        selected_graphs=selected_graphs,
        selected_cpus=selected_cpus,
        selected_disks=selected_disks,
        selected_ifaces=selected_ifaces,
        graph_options=GRAPH_OPTIONS,
        active_sar_name=active_sar_name,
        timezone=timezone,
        selected_cpu_metrics=selected_cpu_metrics,
        selected_disk_metrics=selected_disk_metrics,
        selected_network_metrics=selected_network_metrics,
        iface_filter=iface_filter,
        disk_filter=disk_filter,
        graph_labels=GRAPH_LABELS,
    )


if __name__ == "__main__":
    _ensure_sa_folder()
    app.run(debug=True)