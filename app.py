import os
import pandas as pd
from flask import Flask, render_template, request

from cpu_module import get_cpu_data
from memory_module import get_memory_data
from disk_module import get_disk_data
from network_module import get_network_data
from network_edev_module import get_network_edev_data
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


GRAPH_OPTIONS = ["cpu", "memory", "disk", "network", "network_edev"]


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
    error_message = None
    hostname = ""
    cpu_list = []
    iface_list = []
    disk_list = []
    selected_graphs = list(GRAPH_OPTIONS)
    selected_cpus = ["all"]
    selected_ifaces = []
    selected_disks = []

    sa_files = sorted(_get_sa_files())

    tz = "UTC"
    selected = None
    if request.method == "POST":
        tz = request.form.get("timezone") or "UTC"
        selected_graphs = request.form.getlist("graphs") or list(GRAPH_OPTIONS)
        selected_cpus = request.form.getlist("cpus")
        selected_ifaces = request.form.getlist("ifaces")
        selected_disks = request.form.getlist("disks")
        network_metrics = request.form.getlist("network_metrics")  # e.g. ["bond0::rxpck_s", "vlan100::txkB_s"]
        disk_metrics = request.form.getlist("disk_metrics")  # e.g. ["sda::await", "sdb::util"]
        cpu_metrics = request.form.getlist("cpu_metrics")  # e.g. ["79_system", "20_iowait"]

        selected = request.form.get("selected_file")
        path = os.path.join(SA_FOLDER, selected) if selected and selected in sa_files else None

        if path and os.path.isfile(path):
            try:
                hostname = get_hostname(path) or "(unknown)"
                cpu = None
                if "cpu" in selected_graphs:
                    cpu = get_cpu_data(path, "UTC", tz)
                    def _cpu_sort_key(x):
                        if x == "all":
                            return (0, 0)
                        try:
                            return (1, int(x))
                        except (ValueError, TypeError):
                            return (1, float("inf"))
                    cpu_list = sorted(cpu["cpu"].unique().tolist(), key=_cpu_sort_key)
                    if not selected_cpus:
                        selected_cpus = ["all"] if "all" in cpu_list else cpu_list[:1]

                if "network" in selected_graphs or "network_edev" in selected_graphs:
                    try:
                        net_tmp = get_network_data(path, "UTC", tz)
                        if not net_tmp.empty:
                            iface_list.extend(sorted(net_tmp["iface"].unique().tolist()))
                        elif "network_edev" in selected_graphs:
                            net_edev_tmp = get_network_edev_data(path, "UTC", tz)
                            if not net_edev_tmp.empty:
                                iface_list.extend(sorted(net_edev_tmp["iface"].unique().tolist()))
                    except Exception:
                        pass

                if not selected_ifaces and iface_list:
                    selected_ifaces = iface_list[:]  # default: all interfaces

                if "disk" in selected_graphs:
                    try:
                        disk_tmp = get_disk_data(path, "UTC", tz)
                        if not disk_tmp.empty and "device" in disk_tmp.columns:
                            disk_list.extend(sorted(disk_tmp["device"].unique().tolist()))
                    except Exception:
                        pass

                if not selected_disks and disk_list:
                    selected_disks = disk_list[:]  # default: all disks

                if "cpu" in selected_graphs and cpu is not None and not cpu.empty:
                    cpu_df = cpu[cpu["cpu"].isin(selected_cpus)] if selected_cpus else cpu
                    cpu_df = cpu_df.copy()
                    for col in ["user", "system", "iowait", "steal", "idle"]:
                        cpu_df[col] = pd.to_numeric(cpu_df[col], errors="coerce").fillna(0)
                    cpu_df["total"] = cpu_df["user"] + cpu_df["system"] + cpu_df["iowait"] + cpu_df["steal"]

                    if not cpu_df.empty:
                        name_map = {"user": "%user", "system": "%system", "iowait": "%iowait", "steal": "%steal", "idle": "%idle", "total": "Total"}
                        # Parse custom CPU+metric selections (e.g. "79_system", "20_iowait")
                        custom_pairs = []
                        if cpu_metrics:
                            for v in cpu_metrics:
                                if "_" in v:
                                    cpu_id, metric = v.split("_", 1)
                                    if metric in name_map and (cpu_id in cpu_df["cpu"].values or str(cpu_id) in cpu_df["cpu"].astype(str).values):
                                        custom_pairs.append((str(cpu_id), metric))

                        if custom_pairs:
                            # Plot only the selected CPU+metric combinations
                            cpu_fig = go.Figure()
                            seen_cpus = set()
                            for cpu_id, metric in custom_pairs:
                                sub = cpu_df[cpu_df["cpu"].astype(str) == str(cpu_id)]
                                if sub.empty:
                                    continue
                                label = f"{name_map[metric]} (CPU {cpu_id})"
                                cpu_fig.add_trace(go.Scatter(
                                    x=sub["time"], y=sub[metric],
                                    name=label, mode="lines",
                                    legendgroup=str(cpu_id),
                                    hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}%<extra></extra>",
                                ))
                                seen_cpus.add(cpu_id)
                            if cpu_fig.data:
                                cpu_fig.update_layout(
                                    title="CPU Usage (Selected)",
                                    xaxis_title="Time",
                                    yaxis_title="Usage %",
                                    legend=dict(title="Metric", font=dict(size=10)),
                                )
                                if len(seen_cpus) > 1:
                                    n_traces = len(cpu_fig.data)
                                    buttons = [dict(label="All", method="restyle", args=[{"visible": [True] * n_traces}])]
                                    for c in sorted(seen_cpus, key=lambda x: (0 if x == "all" else 1, int(x) if str(x).isdigit() else 0)):
                                        visible = [getattr(t, "legendgroup", "") == str(c) for t in cpu_fig.data]
                                        buttons.append(dict(label=f"CPU {c}", method="restyle", args=[{"visible": visible}]))
                                    cpu_fig.update_layout(
                                        updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=1.02, xanchor="left", y=1, yanchor="top")],
                                        annotations=[dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)],
                                    )
                                cpu_graph = cpu_fig.to_html(full_html=False)
                        else:
                            # No custom selection: use original behavior
                            if (cpu_df["cpu"] == "all").any() and len(cpu_df["cpu"].unique()) == 1:
                                cpu_plot = cpu_df[cpu_df["cpu"] == "all"].copy()
                            elif len(cpu_df["cpu"].unique()) == 1:
                                cpu_plot = cpu_df.copy()
                            else:
                                cpu_plot = None
                            if cpu_plot is not None:
                                cpu_fig = px.line(
                                    cpu_plot, x="time", y=["user", "system", "iowait", "steal", "idle", "total"],
                                    title="CPU Usage",
                                )
                                cpu_id = cpu_plot["cpu"].iloc[0] if "cpu" in cpu_plot.columns and len(cpu_plot) > 0 else "all"
                                for t in cpu_fig.data:
                                    raw = str(t.name or "").lower()
                                    metric = "Total" if "total" in raw else name_map.get(raw, raw)
                                    t.name = f"{metric} (CPU {cpu_id})"
                                    t.hovertemplate = f"<b>{t.name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}%<extra></extra>"
                                cpu_fig.update_layout(
                                    legend=dict(title="Metric", font=dict(size=11)),
                                    yaxis=dict(title="Usage %"),
                                    annotations=[dict(text=f"CPU: {cpu_id}", xref="paper", yref="paper", x=0.02, y=0.02, showarrow=False, font=dict(size=12))],
                                )
                                cpu_graph = cpu_fig.to_html(full_html=False)
                            else:
                                # Multiple CPUs: all metrics per CPU
                                cpu_fig = go.Figure()
                                metrics = ["user", "system", "iowait", "steal", "idle", "total"]
                                for c in sorted(cpu_df["cpu"].unique(), key=lambda x: (0 if x == "all" else 1, int(x) if str(x).isdigit() else 0)):
                                    sub = cpu_df[cpu_df["cpu"] == c]
                                    for m in metrics:
                                        label = f"{name_map[m]} (CPU {c})"
                                        cpu_fig.add_trace(go.Scatter(
                                            x=sub["time"], y=sub[m],
                                            name=label, mode="lines",
                                            legendgroup=str(c),
                                            hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}%<extra></extra>",
                                        ))
                                cpu_fig.update_layout(
                                    title="CPU Usage (Selected CPUs)",
                                    xaxis_title="Time",
                                    yaxis_title="Usage %",
                                    legend=dict(title="Metric", font=dict(size=10)),
                                )
                                selected_cpu_ids = sorted(
                                    [c for c in cpu_df["cpu"].unique() if c != "all"],
                                    key=lambda x: int(x) if str(x).isdigit() else 0,
                                )
                                if selected_cpu_ids:
                                    n_traces = len(cpu_fig.data)
                                    buttons = [dict(label="All CPUs", method="restyle", args=[{"visible": [True] * n_traces}])]
                                    for c in selected_cpu_ids:
                                        visible = [getattr(t, "legendgroup", "") == str(c) for t in cpu_fig.data]
                                        buttons.append(dict(label=f"CPU {c}", method="restyle", args=[{"visible": visible}]))
                                    cpu_fig.update_layout(
                                        updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=1.02, xanchor="left", y=1, yanchor="top")],
                                        annotations=[dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)],
                                    )
                                cpu_graph = cpu_fig.to_html(full_html=False)

                if "memory" in selected_graphs:
                    mem = get_memory_data(path, "UTC", tz)
                    mem_fig = px.line(
                        mem, x="time", y=["kbmemfree", "kbmemused"],
                        title="Memory Usage",
                    )
                    mem_name_map = {"kbmemused": "Used", "kbmemfree": "Free"}
                    for t in mem_fig.data:
                        t.name = mem_name_map.get(str(t.name), t.name)
                        t.hovertemplate = f"<b>{t.name}</b><br>Time: %{{x}}<br>Value: %{{y:,.0f}} MB<extra></extra>"
                    mem_fig.update_layout(
                        yaxis=dict(
                            title="Memory (MB)",
                            tickformat=",d",
                        ),
                        legend=dict(title="Metric"),
                    )
                    mem_graph = mem_fig.to_html(full_html=False)

                if "disk" in selected_graphs:
                    try:
                        disk = get_disk_data(path, "UTC", tz)
                        disk_df = disk[disk["device"].isin(selected_disks)] if selected_disks else disk
                        if not disk_df.empty:
                            disk_metric_map = {"areq_sz": "areq-sz", "aqu_sz": "aqu-sz", "await": "await", "util": "%util"}
                            custom_disk = []
                            if disk_metrics:
                                for v in disk_metrics:
                                    if "::" in v:
                                        dev, metric = v.split("::", 1)
                                        if metric in disk_metric_map and (dev in disk_df["device"].values or dev in disk_df["device"].astype(str).values):
                                            if metric in disk_df.columns:
                                                custom_disk.append((dev, metric))

                            disk_fig = go.Figure()
                            if custom_disk:
                                for dev, metric in custom_disk:
                                    sub = disk_df[disk_df["device"].astype(str) == str(dev)]
                                    if sub.empty or metric not in sub.columns:
                                        continue
                                    label = f"{disk_metric_map.get(metric, metric)} ({dev})"
                                    disk_fig.add_trace(go.Scatter(
                                        x=sub["time"], y=sub[metric],
                                        name=label, mode="lines",
                                        legendgroup=str(dev),
                                        hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                    ))
                            else:
                                disk_metrics_default = []
                                if "areq_sz" in disk_df.columns:
                                    disk_metrics_default.append("areq_sz")
                                if "aqu_sz" in disk_df.columns:
                                    disk_metrics_default.append("aqu_sz")
                                disk_metrics_default.extend(["await", "util"])
                                disk_metrics_default = [c for c in disk_metrics_default if c in disk_df.columns]
                                for dev in sorted(disk_df["device"].unique()):
                                    sub = disk_df[disk_df["device"] == dev]
                                    for m in disk_metrics_default:
                                        label = f"{disk_metric_map.get(m, m)} ({dev})"
                                        disk_fig.add_trace(go.Scatter(
                                            x=sub["time"], y=sub[m],
                                            name=label, mode="lines",
                                            legendgroup=str(dev),
                                            hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                        ))
                            if disk_fig.data:
                                disk_fig.update_layout(
                                    title="Disk (sar -d)",
                                    xaxis_title="Time",
                                    yaxis_title="Value",
                                    legend=dict(title="Device", font=dict(size=10)),
                                )
                                disks_in_fig = sorted(disk_df["device"].unique())
                                if len(disks_in_fig) > 1:
                                    n_traces = len(disk_fig.data)
                                    buttons = [dict(label="All", method="restyle", args=[{"visible": [True] * n_traces}])]
                                    for d in disks_in_fig:
                                        visible = [getattr(t, "legendgroup", "") == str(d) for t in disk_fig.data]
                                        buttons.append(dict(label=d, method="restyle", args=[{"visible": visible}]))
                                    disk_fig.update_layout(
                                        updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=1.02, xanchor="left", y=1, yanchor="top")],
                                        annotations=[dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)],
                                    )
                                disk_graph = disk_fig.to_html(full_html=False)
                    except Exception:
                        pass

                if "network" in selected_graphs:
                    try:
                        net = get_network_data(path, "UTC", tz)
                        net_df = net[net["iface"].isin(selected_ifaces)] if selected_ifaces else net
                        if not net_df.empty:
                            net_metric_map = {"rxpck_s": "rxpck/s", "txpck_s": "txpck/s", "rxkB_s": "rxkB/s", "txkB_s": "txkB/s", "rxcmp_s": "rxcmp/s", "txcmp_s": "txcmp/s", "rxmcst_s": "rxmcst/s", "ifutil": "%ifutil"}
                            custom_net = []
                            if network_metrics:
                                for v in network_metrics:
                                    if "::" in v:
                                        iface, metric = v.split("::", 1)
                                        if metric in net_metric_map and (iface in net_df["iface"].values or iface in net_df["iface"].astype(str).values):
                                            custom_net.append((iface, metric))

                            net_fig = go.Figure()
                            if custom_net:
                                for iface, metric in custom_net:
                                    sub = net_df[net_df["iface"].astype(str) == str(iface)]
                                    if sub.empty or metric not in sub.columns:
                                        continue
                                    label = f"{net_metric_map.get(metric, metric)} ({iface})"
                                    net_fig.add_trace(go.Scatter(
                                        x=sub["time"], y=sub[metric],
                                        name=label, mode="lines",
                                        legendgroup=str(iface),
                                        hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                    ))
                            else:
                                net_metrics_default = ["rxpck_s", "txpck_s", "rxkB_s", "txkB_s", "ifutil"]
                                for iface in sorted(net_df["iface"].unique()):
                                    sub = net_df[net_df["iface"] == iface]
                                    for m in net_metrics_default:
                                        if m in sub.columns:
                                            label = f"{net_metric_map.get(m, m)} ({iface})"
                                            net_fig.add_trace(go.Scatter(
                                                x=sub["time"], y=sub[m],
                                                name=label, mode="lines",
                                                legendgroup=str(iface),
                                                hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                            ))
                            if net_fig.data:
                                net_fig.update_layout(
                                    title="Network (sar -n DEV)",
                                    xaxis_title="Time",
                                    yaxis_title="Value",
                                    legend=dict(title="Interface", font=dict(size=10)),
                                )
                                ifaces_in_net = sorted(net_df["iface"].unique())
                                if len(ifaces_in_net) > 1:
                                    n_traces = len(net_fig.data)
                                    buttons = [dict(label="All", method="restyle", args=[{"visible": [True] * n_traces}])]
                                    for i in ifaces_in_net:
                                        visible = [getattr(t, "legendgroup", "") == str(i) for t in net_fig.data]
                                        buttons.append(dict(label=i, method="restyle", args=[{"visible": visible}]))
                                    net_fig.update_layout(
                                        updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=1.02, xanchor="left", y=1, yanchor="top")],
                                        annotations=[dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)],
                                    )
                                net_graph = net_fig.to_html(full_html=False)
                    except Exception:
                        pass

                if "network_edev" in selected_graphs:
                    try:
                        net_edev = get_network_edev_data(path, "UTC", tz)
                        net_edev_df = net_edev[net_edev["iface"].isin(selected_ifaces)] if selected_ifaces else net_edev
                        if not net_edev_df.empty:
                            edev_metric_map = {"rxerr_s": "rxerr/s", "txerr_s": "txerr/s", "coll_s": "coll/s", "rxdrop_s": "rxdrop/s", "txdrop_s": "txdrop/s", "txcarr_s": "txcarr/s", "rxfram_s": "rxfram/s", "rxfifo_s": "rxfifo/s", "txfifo_s": "txfifo/s"}
                            edev_metrics_default = ["rxerr_s", "txerr_s", "rxdrop_s", "txdrop_s", "rxfram_s", "rxfifo_s", "txfifo_s"]
                            edev_metrics_default = [c for c in edev_metrics_default if c in net_edev_df.columns]
                            if edev_metrics_default:
                                net_err_fig = go.Figure()
                                for iface in sorted(net_edev_df["iface"].unique()):
                                    sub = net_edev_df[net_edev_df["iface"] == iface]
                                    for m in edev_metrics_default:
                                        label = f"{edev_metric_map.get(m, m)} ({iface})"
                                        net_err_fig.add_trace(go.Scatter(
                                            x=sub["time"], y=sub[m],
                                            name=label, mode="lines",
                                            legendgroup=str(iface),
                                            hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                                        ))
                                if net_err_fig.data:
                                    net_err_fig.update_layout(
                                        title="Network Errors (sar -n EDEV)",
                                        xaxis_title="Time",
                                        yaxis_title="Value",
                                        legend=dict(title="Interface", font=dict(size=10)),
                                    )
                                    ifaces_in_edev = sorted(net_edev_df["iface"].unique())
                                    if len(ifaces_in_edev) > 1:
                                        n_traces = len(net_err_fig.data)
                                        buttons = [dict(label="All", method="restyle", args=[{"visible": [True] * n_traces}])]
                                        for i in ifaces_in_edev:
                                            visible = [getattr(t, "legendgroup", "") == str(i) for t in net_err_fig.data]
                                            buttons.append(dict(label=i, method="restyle", args=[{"visible": visible}]))
                                        net_err_fig.update_layout(
                                            updatemenus=[dict(buttons=buttons, direction="down", showactive=True, x=1.02, xanchor="left", y=1, yanchor="top")],
                                            annotations=[dict(text="Filter:", x=1.02, y=1.05, xref="paper", yref="paper", showarrow=False)],
                                        )
                                    net_err_graph = net_err_fig.to_html(full_html=False)
                    except Exception:
                        pass

            except Exception as e:
                error_message = str(e)
        else:
            error_message = "Please select a SAR file from the sa folder."
    else:
        selected = None

    selected_cpu_metrics = request.form.getlist("cpu_metrics") if request.method == "POST" else []
    selected_network_metrics = request.form.getlist("network_metrics") if request.method == "POST" else []
    selected_disk_metrics = request.form.getlist("disk_metrics") if request.method == "POST" else []

    return render_template(
        "index.html",
        cpu_graph=cpu_graph,
        mem_graph=mem_graph,
        disk_graph=disk_graph,
        net_graph=net_graph,
        net_err_graph=net_err_graph,
        sa_files=sa_files,
        error_message=error_message,
        hostname=hostname,
        cpu_list=cpu_list,
        iface_list=iface_list,
        disk_list=disk_list,
        selected_graphs=selected_graphs,
        selected_cpus=selected_cpus,
        selected_ifaces=selected_ifaces,
        selected_disks=selected_disks,
        selected_cpu_metrics=selected_cpu_metrics,
        selected_network_metrics=selected_network_metrics,
        selected_disk_metrics=selected_disk_metrics,
        graph_options=GRAPH_OPTIONS,
        selected_file=selected if request.method == "POST" else None,
        timezone=tz,
    )


if __name__ == "__main__":
    _ensure_sa_folder()
    app.run(debug=True)