FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*
COPY . /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod +x /app/kafka_startup.sh

ENV PYTHONPATH=/app

ENTRYPOINT ["/app/kafka_startup.sh"]