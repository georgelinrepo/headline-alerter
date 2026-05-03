FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir yoyo-migrations==9.0.0 "psycopg[binary]>=3.2.0"
COPY migrations /app/migrations
CMD ["sh", "-c", "yoyo apply --batch --database \"$POSTGRES_URL\" /app/migrations"]
