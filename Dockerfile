FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    RELAYQUEUE_DB_PATH=/data/relayqueue.db
WORKDIR /app
COPY pyproject.toml README.md ./
COPY relayqueue ./relayqueue
COPY demo ./demo
RUN pip install --no-cache-dir . && mkdir -p /data && useradd -r -u 10001 relay && chown relay /data
USER relay
VOLUME ["/data"]
EXPOSE 8000
CMD ["python", "-m", "relayqueue", "api", "--host", "0.0.0.0", "--port", "8000"]
