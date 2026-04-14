FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=5000

WORKDIR /app

# sysstat provides the `sar` binary used by the parser modules.
RUN apt-get update \
    && apt-get install -y --no-install-recommends sysstat \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install -r /app/requirements.txt

COPY . /app

EXPOSE 5000

CMD ["python", "-c", "import os; from app import app; app.run(host='0.0.0.0', port=int(os.getenv('PORT', '5000')))"]
