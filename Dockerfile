FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY . .

# Default DB location (override with DB_PATH env var; on Railway use a volume at /data)
ENV DB_PATH=/data/paper_trades.db
ENV LOG_LEVEL=INFO

# Create data dir for SQLite (Railway volume should be mounted here)
RUN mkdir -p /data

CMD ["python", "main.py"]
