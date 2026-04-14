# sarInsight 

**sarInsight** visualizes Linux `sar` (sysstat) binary archive files. It parses SAR data with the system `sar` command and plots CPU, memory, disk, network, socket, and process metrics.

## Requirements

- **Python 3.11+** (see `requirements.txt`)

## Installation

There are 2 methods to use this application 

1. 
    ```bash
    cd sarInsight
    python -m venv .venv
    source .venv/bin/activate   # Windows: .venv\Scripts\activate
    pip install -r requirements.txt
    ```
    The Flask app serves interactive Plotly charts and supports multiple SAR files, time zones, and per-metric filters.

    ```bash
    python -c "from app import app; app.run(debug=True)"
    ```

    Or set `FLASK_APP=app` and use `flask run` if you prefer.

    Open the URL shown in the terminal (default `http://127.0.0.1:5000` or `http://localhost:5000`).


2. Docker or Podman

    Build and run the web app in a container (listens on port 5000):

    ```bash
    docker build -t sarinsight .
    docker run -d -p 5000:5000 sarinsight
    ```

Choose the port with `-e PORT=8080` and map `-p 8080:8080` if needed.

start with different port:
```bash
podman run -d -e PORT=8000 -p 8000:8000  sarinsight
docker run -d -e PORT=8000 -p 8000:8000  sarinsight
```