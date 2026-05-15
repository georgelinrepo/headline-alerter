FROM python:3.12-slim
WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir \
        "confluent-kafka>=2.4.0" \
        "psycopg[binary]>=3.2.0" \
        "structlog>=24.0.0" \
        "feedparser>=6.0.11" \
        "truthbrush>=0.3.0"

COPY services /app/services

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# The ingestor module is selected at runtime via the CMD argument.
# In Phase 1a we only ship cnbc_rss; later phases add more.
CMD ["python", "-m", "services.ingestors.cnbc_rss.main"]
