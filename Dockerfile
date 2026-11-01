# syntax=docker/dockerfile:1.7
FROM python:3.13-slim

WORKDIR /immich_drop

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# tzdata lets TZ=Europe/Berlin etc. work (EXIF dates without offset use the container timezone)
RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /immich_drop/requirements.txt
RUN pip install --no-cache-dir -r /immich_drop/requirements.txt

# Copy app code
COPY main.py /immich_drop/main.py
COPY app /immich_drop/app
COPY frontend /immich_drop/frontend

# Data dir for SQLite (state.db) and chunk uploads
RUN mkdir -p /data
VOLUME ["/data"]

# Defaults (can be overridden via compose env)
ENV HOST=0.0.0.0 \
    PORT=8080 \
    STATE_DB=/data/state.db \
    TZ=UTC

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os,urllib.request; urllib.request.urlopen(f'http://127.0.0.1:{os.getenv(\"PORT\",\"8080\")}/login', timeout=3)" || exit 1

CMD ["python", "main.py"]
