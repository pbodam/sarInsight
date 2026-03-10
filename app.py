from flask import Flask, render_template, request
import os

from cpu_module import get_cpu_data
from memory_module import get_memory_data
from disk_module import get_disk_data
from sar_parser import get_hostname

import plotly.express as px

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


GRAPH_OPTIONS = ["cpu", "memory", "disk"]


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
    error_message = None
    hostname = ""
    cpu_list = []
    selected_graphs = list(GRAPH_OPTIONS)
    selected_cpus = ["all"]

    sa_files = sorted(_get_sa_files())

    if request.method == "POST":
        tz = request.form.get("timezone", "UTC")
        selected_graphs = request.form.getlist("graphs") or list(GRAPH_OPTIONS)
        selected_cpus = request.form.getlist("cpus")

        selected = request.form.get("selected_file")
        path = os.path.join(SA_FOLDER, selected) if selected and selected in sa_files else None

        if path and os.path.isfile(path):
            try:
                hostname = get_hostname(path) or "(unknown)"
                cpu = get_cpu_data(path, "UTC", tz)
                cpu_list = sorted(cpu["cpu"].unique().tolist(), key=lambda x: (x == "all", x))
                if not selected_cpus:
                    selected_cpus = ["all"] if "all" in cpu_list else cpu_list[:1]

                cpu_df = cpu[cpu["cpu"].isin(selected_cpus)] if selected_cpus else cpu
                cpu_df = cpu_df.copy()
                cpu_df["total"] = cpu_df["user"] + cpu_df["system"] + cpu_df["iowait"] + cpu_df["steal"]

                if "cpu" in selected_graphs and not cpu_df.empty:
                    if (cpu_df["cpu"] == "all").any():
                        cpu_plot = cpu_df[cpu_df["cpu"] == "all"].copy()
                    elif len(cpu_df["cpu"].unique()) == 1:
                        cpu_plot = cpu_df.copy()
                    else:
                        cpu_plot = cpu_df.groupby("time")[["user","system","iowait","steal","idle"]].mean(numeric_only=True).reset_index()
                        cpu_plot["total"] = cpu_plot["user"] + cpu_plot["system"] + cpu_plot["iowait"] + cpu_plot["steal"]
                    cpu_fig = px.line(
                        cpu_plot, x="time", y=["user", "system", "iowait", "steal", "idle", "total"],
                        title="CPU Usage",
                    )
                    name_map = {"user": "%user", "system": "%system", "iowait": "%iowait", "steal": "%steal", "idle": "%idle", "total": "Total"}
                    for t in cpu_fig.data:
                        raw = str(t.name or "").lower()
                        t.name = "Total" if "total" in raw else name_map.get(raw, raw)
                        t.hovertemplate = f"<b>{t.name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}%<extra></extra>"
                    cpu_fig.update_layout(
                        legend=dict(title="Metric", font=dict(size=11)),
                        yaxis=dict(title="Usage %"),
                    )
                    cpu_graph = cpu_fig.to_html(full_html=False)

                if "memory" in selected_graphs:
                    mem = get_memory_data(path, "UTC", tz)
                    mem_cols = [c for c in ["kbmemfree", "kbmemused"] if c in mem.columns]
                    if mem_cols:
                        mem_fig = px.line(mem, x="time", y=mem_cols, title="Memory Usage")
                    else:
                        mem_fig = px.line(mem, x="time", y="time", title="Memory Usage")
                    name_map = {"kbmemfree": "Free (MB)", "kbmemused": "Used (MB)"}
                    for t in mem_fig.data:
                        t.name = name_map.get(str(t.name), str(t.name))
                    mem_graph = mem_fig.to_html(full_html=False)

                if "disk" in selected_graphs:
                    disk = get_disk_data(path, "UTC", tz)
                    disk_fig = px.line(disk, x="time", y=["await", "util"], title="Disk (Latency &amp; Util)")
                    disk_graph = disk_fig.to_html(full_html=False)

            except Exception as e:
                error_message = str(e)
        else:
            error_message = "Please select a SAR file from the sa folder."
    else:
        selected = None

    return render_template(
        "index.html",
        cpu_graph=cpu_graph,
        mem_graph=mem_graph,
        disk_graph=disk_graph,
        net_graph=None,
        net_err_graph=None,
        sa_files=sa_files,
        error_message=error_message,
        hostname=hostname,
        cpu_list=cpu_list,
        selected_graphs=selected_graphs,
        selected_cpus=selected_cpus,
        graph_options=GRAPH_OPTIONS,
        selected_file=selected if request.method == "POST" else None,
    )


if __name__ == "__main__":
    _ensure_sa_folder()
    app.run(debug=True)