FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install sar command support (sysstat) used by the parser.
RUN apt-get update && apt-get install -y --no-install-recommends \
    sysstat \
    && rm -rf /var/lib/apt/lists/*

# Install only web runtime dependencies (GUI deps intentionally excluded).
RUN pip install --upgrade pip && pip install \
    flask \
    pandas \
    plotly \
    pytz \
    gunicorn

COPY . .

# Create data folders expected by the app.
RUN mkdir -p /app/sa /app/sos

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "app:app"]
