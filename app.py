from flask import Flask, render_template, request
import os

import pandas as pd

from cpu_module import get_cpu_data
from memory_module import get_memory_data
from disk_module import get_disk_data
from network_module import get_network_data
from network_edev_module import get_network_edev_data
from socket_module import get_socket_data
from sar_parser import get_hostname

import plotly.express as px
import plotly.graph_objects as go

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SA_FOLDER = os.path.join(BASE_DIR, "sa")

app = Flask(__name__)
app.config["SA_FOLDER"] = SA_FOLDER


def _ensure_sa_folder():
    os.makedirs(SA_FOLDER, exist_ok=True)


def _get_sa_files():
    _ensure_sa_folder()
    try:
        return [f for f in os.listdir(SA_FOLDER) if os.path.isfile(os.path.join(SA_FOLDER, f))]
    except OSError:
        return []


GRAPH_OPTIONS = ["cpu", "memory", "disk", "network", "network_errors", "sockets"]

# Shown in the form, page section headings, and Plotly chart titles.
GRAPH_LABELS = {
    "cpu": "CPU utilization",
    "memory": "Memory usage",
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
    "totsck": "Total sockets",
    "tcpsck": "TCP in use",
    "udpsck": "UDP in use",
    "rawsck": "RAW sockets",
    "ip_frag": "IP fragments in use",
    "tcp_tw": "TCP TIME-WAIT",
}

MEM_LEGEND_LABELS = {
    "kbmemfree": "Free memory (kB)",
    "kbmemused": "Used memory (kB)",
}


def _load_sa_data(path: str, tz: str):
    """Load CPU, disk, and network frames plus unique ids for filter UI (best-effort)."""
    cpu = disk = net = None
    cpu_list, disk_list, iface_list = [], [], []
    try:
        cpu = get_cpu_data(path, "UTC", tz)
        if cpu is not None and not cpu.empty and "cpu" in cpu.columns:
            cpu_list = sorted(cpu["cpu"].unique().tolist(), key=lambda x: (x == "all", x))
    except Exception:
        cpu = None
    try:
        disk = get_disk_data(path, "UTC", tz)
        if disk is not None and "device" in disk.columns and not disk.empty:
            disk_list = sorted(disk["device"].dropna().astype(str).unique().tolist())
    except Exception:
        disk = None
    try:
        net = get_network_data(path, "UTC", tz)
        if net is not None and "iface" in net.columns and not net.empty:
            iface_list = sorted(net["iface"].dropna().astype(str).unique().tolist())
    except Exception:
        net = None
    return cpu, disk, net, cpu_list, disk_list, iface_list


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


@app.route("/", methods=["GET", "POST"])
def index():
    cpu_graph = None
    mem_graph = None
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
    selected_cpu_metrics = []
    selected_disk_metrics = []
    selected_network_metrics = []
    timezone = "UTC"
    selected = None

    sa_files = sorted(_get_sa_files())

    if request.method == "GET":
        preview = request.args.get("preview_file")
        if preview and preview in sa_files:
            selected = preview
            timezone = request.args.get("timezone", "UTC")
            path_prev = os.path.join(SA_FOLDER, preview)
            if os.path.isfile(path_prev):
                try:
                    hostname = get_hostname(path_prev) or "(unknown)"
                except Exception:
                    hostname = ""
                _, _, _, cpu_list, disk_list, iface_list = _load_sa_data(path_prev, timezone)

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

        selected = request.form.get("selected_file")
        path = os.path.join(SA_FOLDER, selected) if selected and selected in sa_files else None

        if path and os.path.isfile(path):
            try:
                hostname = get_hostname(path) or "(unknown)"
                cpu, disk, net, cpu_list, disk_list, iface_list = _load_sa_data(path, tz)
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
                            cpu_graph = cpu_fig.to_html(full_html=False)

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
                        cpu_graph = cpu_fig.to_html(full_html=False)

                if "memory" in selected_graphs:
                    mem = get_memory_data(path, "UTC", tz)
                    mem_cols = [c for c in ["kbmemfree", "kbmemused"] if c in mem.columns]
                    if mem_cols:
                        mem_fig = px.line(mem, x="time", y=mem_cols, title=GRAPH_LABELS["memory"])
                    else:
                        mem_fig = px.line(mem, x="time", y="time", title=GRAPH_LABELS["memory"])
                    name_map = MEM_LEGEND_LABELS
                    for t in mem_fig.data:
                        t.name = name_map.get(str(t.name), str(t.name))
                    if mem_cols:
                        mem_fig.update_layout(yaxis_title="Kilobytes")
                    mem_graph = mem_fig.to_html(full_html=False)

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
                            disk_graph = disk_fig.to_html(full_html=False)
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
                            disk_graph = disk_fig.to_html(full_html=False)

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
                            net_graph = net_fig.to_html(full_html=False)
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
                            net_graph = net_fig.to_html(full_html=False)

                if "network_errors" in selected_graphs:
                    edev = get_network_edev_data(path, "UTC", tz)
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
                            net_err_graph = net_err_fig.to_html(full_html=False)

                if "sockets" in selected_graphs:
                    sock = get_socket_data(path, "UTC", tz)
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
                            sock_fig = px.line(
                                sock,
                                x="time",
                                y=sock_cols,
                                title=GRAPH_LABELS["sockets"],
                                labels={
                                    c: SOCKET_LEGEND_LABELS.get(c, c) for c in sock_cols
                                },
                            )
                            sock_graph = sock_fig.to_html(full_html=False)

            except Exception as e:
                error_message = str(e)
        else:
            error_message = "Please select a SAR file from the sa folder."

    return render_template(
        "index.html",
        cpu_graph=cpu_graph,
        mem_graph=mem_graph,
        disk_graph=disk_graph,
        net_graph=net_graph,
        net_err_graph=net_err_graph,
        sock_graph=sock_graph,
        sa_files=sa_files,
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
        selected_file=selected,
        timezone=timezone,
        selected_cpu_metrics=selected_cpu_metrics,
        selected_disk_metrics=selected_disk_metrics,
        selected_network_metrics=selected_network_metrics,
        graph_labels=GRAPH_LABELS,
    )


if __name__ == "__main__":
    _ensure_sa_folder()
    app.run(debug=True)