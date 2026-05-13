FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "fastapi>=0.115.0" \
        "uvicorn[standard]>=0.30.0" \
        "sse-starlette>=2.0.0"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "uvicorn", "services.dashboard.api:app", "--host", "0.0.0.0", "--port", "8000"]
