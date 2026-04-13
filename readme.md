# sarInsight 

**sarInsight** visualizes Linux `sar` (sysstat) binary archive files. It parses SAR data with the system `sar` command and plots CPU, memory, disk, network, socket, and process metrics.

## Requirements

- **Python 3.11+** (see `requirements.txt`)

## Installation

```bash
cd sarInsight
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Web application (recommended)

The Flask app serves interactive Plotly charts and supports multiple SAR files, time zones, and per-metric filters.

1. Place one or more SAR archive files in the `sa/` directory at the project root (the app creates `sa/` if missing).
2. Run:

```bash
python -c "from app import app; app.run(debug=True)"
```

Or set `FLASK_APP=app` and use `flask run` if you prefer.

3. Open the URL shown in the terminal (default `http://127.0.0.1:5000` or `http://localhost:5000`).

**Features:**

- Choose a SAR file, display timezone, and which graphs to show: CPU, memory, disk, network, network errors (`network_edev`), sockets, total process count.
- CPU graphs can filter by CPU id; network and disk views support metric/device selection where applicable.

SAR files are read from the `sa/` directory next to `app.py`. To use another path, change `SA_FOLDER` in `app.py`.


Use **Load SAR File** to pick a SAR file from disk. Choose a display timezone from the combo box.

## Docker or Podman

Build and run the web app in a container (listens on port 5000):

```bash
docker build -t sarinsight .
docker run --rm -p 5000:5000 -v /path/to/your/sar/files:/app/sa sarinsight
```

Mount your SAR archives into `/app/sa` so they appear in the web UI. Override the port with `-e PORT=8080` and map `-p 8080:8080` if needed.

start with different port:
```bash
podman run --rm -e PORT=8000 -p 8000:8000 -v ./sa:/app/sa  sarinsight
docker run --rm -e PORT=8000 -p 8000:8000 -v ./sa:/app/sa  sarinsight
```

## Project layout

| Component | Role |
|-----------|------|
| `app.py` | Flask routes, Plotly figures, HTML templates |
| `main.py` / `gui.py` | PyQt5 entry point and plots |
| `sar_parser.py` | Runs `sar` via subprocess, parses lines, hostname and time helpers |
| `cpu_module.py` | CPU utilization from SAR |
| `memory_module.py` | Memory statistics |
| `disk_module.py` | Disk / block device metrics |
| `network_module.py` | Per-interface network throughput |
| `network_edev_module.py` | Network errors/drops (edev) |
| `socket_info.py` | Socket-related SAR data |
| `total_process_count.py` | Process count time series |
| `sa/` | Default folder for SAR files (web UI) |
| `templates/` | Flask HTML templates |
