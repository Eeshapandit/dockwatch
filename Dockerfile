# One image for every pipeline job: producer, consumer, trips loader, dbt,
# exporter and the dashboard. Each compose service just runs a different command.
# Pinning Python here (3.12) is what makes it run the same on every machine.
FROM python:3.12-slim

WORKDIR /app
COPY requirements-app.txt .
RUN pip install --no-cache-dir -r requirements-app.txt

COPY . .
ENV PYTHONUNBUFFERED=1 \
    PG_DSN=postgresql://dockwatch:dockwatch@postgres:5432/dockwatch \
    DBT_HOST=postgres \
    KAFKA_BOOTSTRAP=redpanda:9092
