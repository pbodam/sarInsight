import os
import re
import tarfile
import fnmatch
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
SOS_FOLDER = os.path.join(BASE_DIR, "sos")

app = Flask(__name__)
app.config["SA_FOLDER"] = SA_FOLDER
app.config["SOS_FOLDER"] = SOS_FOLDER


def _ensure_sa_folder():
    os.makedirs(SA_FOLDER, exist_ok=True)


def _ensure_sos_folder():
    os.makedirs(SOS_FOLDER, exist_ok=True)


def _get_sa_files():
    _ensure_sa_folder()
    try:
        return [f for f in os.listdir(SA_FOLDER) if os.path.isfile(os.path.join(SA_FOLDER, f))]
    except OSError:
        return []


def _get_sos_files():
    _ensure_sos_folder()
    try:
        allowed_exts = (".tar", ".tar.gz", ".tgz", ".tar.xz", ".txz", ".tar.bz2", ".tbz2")
        items = []
        for f in os.listdir(SOS_FOLDER):
            full = os.path.join(SOS_FOLDER, f)
            if os.path.isdir(full):
                items.append(f)
            elif os.path.isfile(full) and f.lower().endswith(allowed_exts):
                items.append(f)
        return items
    except OSError:
        return []


def _extract_from_tar(archive_path, candidate_suffixes):
    try:
        with tarfile.open(archive_path, "r:*") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                lower_name = member.name.lower()
                if any(lower_name.endswith(sfx.lower()) for sfx in candidate_suffixes):
                    extracted = tar.extractfile(member)
                    if not extracted:
                        continue
                    return extracted.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""
    return ""


def _extract_from_dir(report_dir, candidate_suffixes):
    for suffix in candidate_suffixes:
        rel = suffix.lstrip("/").replace("/", os.sep)
        full = os.path.join(report_dir, rel)
        if os.path.isfile(full):
            try:
                with open(full, "r", encoding="utf-8", errors="ignore") as fp:
                    return fp.read()
            except Exception:
                continue
    return ""


def _path_matches(path_value, suffix_pattern):
    norm_path = "/" + path_value.replace("\\", "/").lstrip("/")
    norm_pattern = "/" + suffix_pattern.replace("\\", "/").lstrip("/")
    if "*" in norm_pattern:
        return fnmatch.fnmatch(norm_path.lower(), f"*{norm_pattern.lower()}")
    return norm_path.lower().endswith(norm_pattern.lower())


def _extract_many_from_tar(archive_path, candidate_suffixes):
    found = []
    try:
        with tarfile.open(archive_path, "r:*") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                if not any(_path_matches(member.name, sfx) for sfx in candidate_suffixes):
                    continue
                extracted = tar.extractfile(member)
                if not extracted:
                    continue
                text = extracted.read().decode("utf-8", errors="ignore")
                found.append((member.name, text))
    except Exception:
        return []
    return found


def _extract_many_from_dir(report_dir, candidate_suffixes):
    found = []
    for root, _, files in os.walk(report_dir):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, report_dir)
            rel_norm = "/" + rel.replace("\\", "/")
            if not any(_path_matches(rel_norm, sfx) for sfx in candidate_suffixes):
                continue
            try:
                with open(full, "r", encoding="utf-8", errors="ignore") as fp:
                    found.append((rel_norm, fp.read()))
            except Exception:
                continue
    return found


def _extract_first_text(report_path, candidate_suffixes):
    if os.path.isdir(report_path):
        for _, text in _extract_many_from_dir(report_path, candidate_suffixes):
            if text.strip():
                return text
        return ""
    for _, text in _extract_many_from_tar(report_path, candidate_suffixes):
        if text.strip():
            return text
    return ""


def _extract_all_text(report_path, candidate_suffixes):
    if os.path.isdir(report_path):
        return _extract_many_from_dir(report_path, candidate_suffixes)
    return _extract_many_from_tar(report_path, candidate_suffixes)


def _truncate_output(text, max_lines=200):
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text.strip() if text.strip() else "No output found."
    clipped = "\n".join(lines[:max_lines]).strip()
    return f"{clipped}\n\n... output truncated ({len(lines) - max_lines} more lines)"


def _filter_messages(messages_text, pattern):
    if not messages_text.strip():
        return "No log lines found in var/log/messages*."
    regex = re.compile(pattern, flags=re.IGNORECASE)
    matches = [ln for ln in messages_text.splitlines() if regex.search(ln)]
    if not matches:
        return "No matches found."
    return _truncate_output("\n".join(matches), max_lines=300)


def parse_sos_report_summary(archive_path):
    summary = {
        "hostname": "",
        "os_release": "",
        "kernel": "",
        "uptime": "",
        "report_date": "",
    }

    hostname_text = _extract_first_text(archive_path, ["/hostname"])
    if hostname_text:
        summary["hostname"] = hostname_text.strip().splitlines()[0] if hostname_text.strip() else ""

    os_release_text = _extract_first_text(archive_path, ["/etc/redhat-release", "/etc/os-release"])
    if os_release_text:
        first_line = next((ln.strip() for ln in os_release_text.splitlines() if ln.strip()), "")
        summary["os_release"] = first_line

    uname_text = _extract_first_text(archive_path, ["/sos_commands/kernel/uname_-a", "/uname"])
    if uname_text:
        summary["kernel"] = next((ln.strip() for ln in uname_text.splitlines() if ln.strip()), "")

    uptime_text = _extract_first_text(archive_path, ["/uptime", "/sos_commands/date/uptime"])
    if uptime_text:
        summary["uptime"] = next((ln.strip() for ln in uptime_text.splitlines() if ln.strip()), "")

    date_text = _extract_first_text(archive_path, ["/sos_commands/date/date"])
    if date_text:
        summary["report_date"] = next((ln.strip() for ln in date_text.splitlines() if ln.strip()), "")

    # Best-effort fallback: search archive name for hostname-like token.
    if not summary["hostname"]:
        filename = os.path.basename(archive_path.rstrip("\\/"))
        m = re.search(r"sosreport-([^-\.]+)", filename, flags=re.IGNORECASE)
        if m:
            summary["hostname"] = m.group(1)

    return summary


def parse_sos_report_entries(report_path):
    command_map = [
        ("uname -a", ["/sos_commands/kernel/uname_-a", "/uname"]),
        ("uptime", ["/sos_commands/date/uptime", "/uptime"]),
        ("/proc/meminfo", ["/proc/meminfo"]),
        ("ps aux", ["/sos_commands/process/ps_aux*", "/ps"]),
        ("var/log/messages", ["/var/log/messages"]),
        ("dmesg", ["/sos_commands/kernel/dmesg", "/dmesg"]),
        ("ip addr", ["/sos_commands/networking/ip_addr*", "/sos_commands/networking/ip_-d_address*"]),
        ("ip route", ["/sos_commands/networking/ip_route*", "/sos_commands/networking/ip_-4_route*"]),
        ("iostat", ["/sos_commands/block/iostat*", "/sos_commands/scsi/iostat*"]),
        ("lsblk", ["/sos_commands/block/lsblk*"]),
        ("sar data", ["/var/log/sa/*", "/sos_commands/sar/*"]),
        ("systemctl --failed", ["/sos_commands/systemd/systemctl_--failed*", "/sos_commands/systemd/systemctl_list-units_--failed*"]),
    ]

    entries = []
    for title, paths in command_map:
        text = _extract_first_text(report_path, paths)
        entries.append(
            {
                "title": title,
                "output": _truncate_output(text) if text else "No data found in SOS report.",
            }
        )

    messages_files = _extract_all_text(report_path, ["/var/log/messages*"])
    messages_text = "\n".join([txt for _, txt in messages_files if txt])

    pattern1 = r"kernel: error|kernel: warning|segfault|kernel panic|soft lockup|hard lockup|hung task|BUG:|Oops:|Call Trace:|tainted|EDAC|I/O error|blk_update_request|read-only file system|EXT4-fs error|EXT4-fs warning|xfs_error|buffer I/O error|SCSI error|device not ready|write error|nic link is down|link is not ready|tx hang|packet dropped|duplicate address|RTNETLINK answers|DHCP timeout|ARP failure|firmware: failed|PCIe Bus Error|uncorrected error|MCE:|ECC error|Out of memory|OOM-killer|page allocation failure|swap exhausted|Failed to start|Unit entered failed state|Restarting too quickly|authentication failure|permission denied|Failed password|TLS handshake failure|timeout waiting|connection reset|too many open files|bind failed|socket error|virtio|KVM: entry failed"
    pattern2 = r"link is down|link is up|NIC Link is Down|NIC Link is Up|carrier lost|carrier recovered|eth.*down|eth.*up|enp.*down|enp.*up|port.*down|port.*up|state DOWN|state UP|rx loss|tx hang|resetting adapter|link flap|link failure|link state change|interface.*down|interface.*up|LACP|bond.*down|bond.*up|Dropped packet|netdev watchdog|PHY.*down|PHY.*up|Speed.*Duplex|mtu mismatch"

    entries.append(
        {
            "title": "egrep critical patterns in var/log/messages*",
            "output": _filter_messages(messages_text, pattern1),
        }
    )
    entries.append(
        {
            "title": "egrep link/network patterns in var/log/messages*",
            "output": _filter_messages(messages_text, pattern2),
        }
    )
    return entries


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
    active_tab = "sar"
    sos_error_message = None
    sos_summary = None
    sos_entries = []
    selected_sos_file = None

    sa_files = sorted(_get_sa_files())
    sos_files = sorted(_get_sos_files())

    tz = "UTC"
    selected = None
    if request.method == "POST":
        active_tab = request.form.get("tab") or "sar"
        if active_tab == "sos":
            selected_sos_file = request.form.get("selected_sos_file")
            sos_path = (
                os.path.join(SOS_FOLDER, selected_sos_file)
                if selected_sos_file and selected_sos_file in sos_files
                else None
            )
            if sos_path and os.path.exists(sos_path):
                sos_summary = parse_sos_report_summary(sos_path)
                sos_entries = parse_sos_report_entries(sos_path)
            else:
                sos_error_message = "Please select a SOS report folder or archive from the sos folder."
            return render_template(
                "index.html",
                active_tab=active_tab,
                sos_files=sos_files,
                selected_sos_file=selected_sos_file,
                sos_summary=sos_summary,
                sos_entries=sos_entries,
                sos_error_message=sos_error_message,
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
                selected_cpu_metrics=[],
                selected_network_metrics=[],
                selected_disk_metrics=[],
                graph_options=GRAPH_OPTIONS,
                selected_file=None,
                timezone=tz,
            )

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
        active_tab=active_tab,
        sos_files=sos_files,
        selected_sos_file=selected_sos_file,
        sos_summary=sos_summary,
        sos_entries=sos_entries,
        sos_error_message=sos_error_message,
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
    _ensure_sos_folder()
    app.run(debug=True)