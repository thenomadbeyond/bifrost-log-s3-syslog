FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    STATE_DB=/data/state.db

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && useradd --system --uid 10001 --home /app shipper \
 && mkdir -p /data && chown shipper /data

COPY shipper.py .

USER 10001
VOLUME ["/data"]
ENTRYPOINT ["python", "/app/shipper.py"]
