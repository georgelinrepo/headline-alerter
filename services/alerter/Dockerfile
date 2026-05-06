FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "twilio>=9.0.0" \
        "httpx>=0.27.0"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "services.alerter.main"]
